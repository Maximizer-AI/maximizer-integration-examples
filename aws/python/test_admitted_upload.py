import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from admitted_upload import AdmittedUploads
import upload_and_fetch as published_client
from botocore.exceptions import ClientError


class AdmittedUploadTests(unittest.TestCase):
    def setUp(self):
        self.s3, self.invoke = Mock(), Mock()
        self.client = AdmittedUploads(self.s3, self.invoke, 'configured-upload', 'eu-central-1')
        self.tmp = tempfile.TemporaryDirectory()
        self.file = Path(self.tmp.name) / 'synthetic.mp3'
        self.file.write_bytes(b'synthetic spoken audio placeholder')
        self.addCleanup(self.tmp.cleanup)
        self.events = []
        self.client.put = Mock(return_value='"' + 'a' * 32 + '"')

    def responses(self, status=200, url='https://demo-bucket.s3.eu-central-1.amazonaws.com/reserved-part', size=None):
        def invoke(**kwargs):
            self.assertEqual(kwargs['FunctionName'], 'configured-upload')
            self.assertEqual(kwargs['InvocationType'], 'RequestResponse')
            event = json.loads(kwargs['Payload'])
            self.events.append(event)
            if event['operation'] == 'start':
                payload = {'status': status, 'error': 'upload_quota_exceeded', 'retry_after': 60,
                    'request_id': event['request_id'], 'parts': [{'part_number': 1, 'size': self.file.stat().st_size if size is None else size, 'url': url}]}
            else:
                payload = {'status': 200, 'completed': True}
            return {'Payload': io.BytesIO(json.dumps(payload).encode())}
        self.invoke.invoke.side_effect = invoke

    def test_upload_uses_admission_transfer_and_one_completion(self):
        self.responses()
        self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.assertEqual([event['operation'] for event in self.events], ['start', 'complete'])
        self.assertEqual(self.events[0]['request_id'], self.events[1]['request_id'])
        self.assertEqual(self.events[1]['etags'], ['"' + 'a' * 32 + '"'])
        self.assertEqual(self.client.put.call_args.args[1], self.file.read_bytes())
        self.s3.upload_file.assert_not_called()

    def test_quota_rejection_has_no_storage_or_direct_fallback(self):
        self.responses(status=429)
        with self.assertRaisesRegex(RuntimeError, '429.*retry after 60s'):
            self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.client.put.assert_not_called()
        self.s3.upload_file.assert_not_called()

    def test_invalid_destinations_never_receive_file_bytes(self):
        for url in ('http://demo-bucket.s3.eu-central-1.amazonaws.com/p', 'https://example.invalid/p',
                    'https://other-bucket.s3.eu-central-1.amazonaws.com/p',
                    'https://demo-bucket.s3.eu-central-1.amazonaws.com:444/p',
                    'https://someone@demo-bucket.s3.eu-central-1.amazonaws.com/p'):
            self.responses(url=url)
            with self.subTest(url=url), self.assertRaisesRegex(RuntimeError, 'Invalid reserved upload destination'):
                self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.client.put.assert_not_called()
        self.s3.upload_file.assert_not_called()

    def test_invalid_size_plan_never_receives_file_bytes(self):
        self.responses(size=7344)
        with self.assertRaisesRegex(RuntimeError, 'Invalid reserved upload plan'):
            self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.client.put.assert_not_called()

    def test_runtime_failure_does_not_print_payload_or_fall_back(self):
        self.invoke.invoke.return_value = {'FunctionError': 'Unhandled', 'Payload': io.BytesIO(b'private-debug-payload')}
        with self.assertRaisesRegex(RuntimeError, 'admission service failed') as caught:
            self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.assertNotIn('private-debug', str(caught.exception))
        self.s3.upload_file.assert_not_called()

    def test_result_reads_keep_using_s3(self):
        self.client.get_object(Bucket='demo-results', Key='category_a/demo.json')
        self.s3.get_object.assert_called_once_with(Bucket='demo-results', Key='category_a/demo.json')
        self.invoke.invoke.assert_not_called()

    def test_failed_part_is_not_completed(self):
        self.responses()
        self.client.put.side_effect = RuntimeError('network unavailable')
        with self.assertRaisesRegex(RuntimeError, 'network unavailable'):
            self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.assertEqual([event['operation'] for event in self.events], ['start'])
        self.s3.upload_file.assert_not_called()

    def test_restarting_exact_file_reuses_request_id(self):
        self.responses()
        self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.assertEqual(self.events[0]['request_id'], self.events[2]['request_id'])
        self.file.write_bytes(b'different synthetic content')
        self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.assertNotEqual(self.events[0]['request_id'], self.events[4]['request_id'])

    def test_same_size_change_after_hash_does_not_complete_old_identity(self):
        self.file.write_bytes(b'old!')
        self.responses()
        original_start = self.invoke.invoke.side_effect
        def changing_start(**kwargs):
            response = original_start(**kwargs)
            if json.loads(kwargs['Payload'])['operation'] == 'start':
                self.file.write_bytes(b'new!')
            return response
        self.invoke.invoke.side_effect = changing_start
        with self.assertRaisesRegex(RuntimeError, 'Local file changed.*completion was not attempted'):
            self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.assertEqual([event['operation'] for event in self.events], ['start'])
        self.assertEqual(self.client.put.call_args.args[1], b'new!')
        self.s3.upload_file.assert_not_called()

    def test_completed_reservation_is_not_uploaded_again(self):
        self.invoke.invoke.return_value = {'Payload': io.BytesIO(b'{"status":200,"completed":true}')}
        self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.client.put.assert_not_called()
        self.s3.upload_file.assert_not_called()

    @patch('admitted_upload.time.sleep')
    def test_transient_admission_failure_retries_the_identical_request(self, sleep):
        def payload(value):
            return {'Payload': io.BytesIO(json.dumps(value).encode())}
        self.invoke.invoke.side_effect = [
            payload({'status': 503, 'error': 'admission_unavailable', 'retry_after': 5}),
            payload({'status': 409, 'error': 'request_in_progress', 'retry_after': 2}),
            payload({'status': 200, 'completed': True}),
        ]
        self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        requests = [call.kwargs['Payload'] for call in self.invoke.invoke.call_args_list]
        self.assertEqual(len(requests), 3)
        self.assertEqual(requests, [requests[0]] * 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [5, 2])
        self.client.put.assert_not_called()
        self.s3.upload_file.assert_not_called()

    @patch('admitted_upload.time.sleep')
    def test_transient_retry_is_bounded_and_never_falls_back(self, sleep):
        self.invoke.invoke.side_effect = [
            {'Payload': io.BytesIO(b'{"status":503,"error":"admission_unavailable","retry_after":5}')}
            for _ in range(3)
        ]
        with self.assertRaisesRegex(RuntimeError, '503 admission_unavailable'):
            self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.assertEqual(self.invoke.invoke.call_count, 3)
        self.assertEqual(sleep.call_count, 2)
        self.client.put.assert_not_called()
        self.s3.upload_file.assert_not_called()


    def test_expired_or_claimed_filename_has_clear_recovery_scope(self):
        for status, code in ((410, 'upload_expired'), (409, 'object_name_already_reserved')):
            with self.subTest(code=code):
                self.invoke.reset_mock()
                self.invoke.invoke.side_effect = None
                self.invoke.invoke.return_value = {'Payload': io.BytesIO(json.dumps({
                    'status': status, 'error': code, 'url': 'private-transfer-url'}).encode())}
                with self.assertRaisesRegex(RuntimeError, 'fresh UCID') as caught:
                    self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
                self.assertNotIn('private-transfer-url', str(caught.exception))
                self.assertEqual(self.invoke.invoke.call_count, 1)
        self.client.put.assert_not_called()
        self.s3.upload_file.assert_not_called()

    @patch('admitted_upload.time.sleep')
    def test_uncertain_incomplete_retry_stays_bounded_and_explains_limit(self, sleep):
        self.invoke.invoke.side_effect = [
            {'Payload': io.BytesIO(b'{"status":409,"error":"request_in_progress","retry_after":2}')}
            for _ in range(3)
        ]
        with self.assertRaisesRegex(RuntimeError, 'fresh UCID if it cannot be recovered'):
            self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        requests = [call.kwargs['Payload'] for call in self.invoke.invoke.call_args_list]
        self.assertEqual(requests, [requests[0]] * 3)
        self.assertEqual(sleep.call_count, 2)
        self.client.put.assert_not_called()
        self.s3.upload_file.assert_not_called()

    def test_unconfirmed_completion_does_not_suggest_automatic_replacement(self):
        self.invoke.invoke.return_value = {'Payload': io.BytesIO(b'{"status":409,"error":"upload_conflict"}')}
        with self.assertRaisesRegex(RuntimeError, 'contact Maximizer with the UCID before replacing'):
            self.client.upload_file(str(self.file), 'demo-bucket', 'category_a__demo.mp3')
        self.assertEqual(self.invoke.invoke.call_count, 1)
        self.client.put.assert_not_called()
        self.s3.upload_file.assert_not_called()


class AdmittedResultReplayTests(unittest.TestCase):
    def setUp(self):
        self.s3, self.sqs = Mock(), Mock()
        self.environment = patch.dict('os.environ', {
            'MAXIMIZER_UPLOAD_FUNCTION': 'configured-upload',
            'MAXIMIZER_RESULTS_QUEUE_URL': 'https://example.invalid/owned-queue'})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        output = patch('builtins.print')
        output.start(); self.addCleanup(output.stop)
        self.sqs.receive_message.return_value = {'Messages': [{
            'Body': json.dumps({'results_file_name': 'category_a/demo.json',
                                'status': 'processed', 'message_timestamp': 'synthetic'}),
            'ReceiptHandle': 'owned-receipt'}]}

    def wait(self):
        published_client._wait_for_result(self.s3, self.sqs, 'demo-results', 'category_a/demo.json')

    def test_completed_result_is_downloaded_without_another_queue_message(self):
        # Consumed completion notification: a replay must not receive or delete
        # any shared-queue messages when its immutable result already exists.
        self.sqs.receive_message.return_value = {'Messages': []}
        self.sqs.receive_message.side_effect = AssertionError('Completed replay must not receive messages')
        self.wait()
        self.s3.head_object.assert_called_once_with(Bucket='demo-results', Key='category_a/demo.json')
        self.s3.download_file.assert_called_once_with('demo-results', 'category_a/demo.json', 'demo.json')
        self.sqs.receive_message.assert_not_called()
        self.sqs.delete_message.assert_not_called()

    def test_not_yet_available_result_still_waits_for_its_notification(self):
        self.s3.head_object.side_effect = ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        self.wait()
        self.sqs.receive_message.assert_called_once()
        self.s3.download_file.assert_called_once_with('demo-results', 'category_a/demo.json', 'demo.json')
        self.sqs.delete_message.assert_called_once_with(
            QueueUrl='https://example.invalid/owned-queue', ReceiptHandle='owned-receipt')

    def test_pending_result_appears_after_consumed_notification_without_second_receive(self):
        self.s3.head_object.side_effect = [
            ClientError({'Error': {'Code': '404'}}, 'HeadObject'), {}]
        self.sqs.receive_message.side_effect = [
            {'Messages': []}, AssertionError('Existing result must prevent another queue receive')]
        self.wait()
        self.assertEqual(self.s3.head_object.call_count, 2)
        self.sqs.receive_message.assert_called_once()
        self.s3.download_file.assert_called_once_with('demo-results', 'category_a/demo.json', 'demo.json')
        self.sqs.delete_message.assert_not_called()

    def test_unreadable_result_does_not_consume_or_delete_queue_messages(self):
        self.s3.head_object.side_effect = ClientError({'Error': {'Code': '403'}}, 'HeadObject')
        with self.assertRaises(ClientError):
            self.wait()
        self.sqs.receive_message.assert_not_called()
        self.sqs.delete_message.assert_not_called()
        self.s3.download_file.assert_not_called()

    def test_direct_ingest_queue_behavior_is_preserved(self):
        with patch.dict('os.environ'):
            published_client.os.environ.pop('MAXIMIZER_UPLOAD_FUNCTION', None)
            self.wait()
        self.s3.head_object.assert_not_called()
        self.sqs.receive_message.assert_called_once()
        self.sqs.delete_message.assert_called_once()


class ResultOnlyCliTests(unittest.TestCase):
    def run_cli(self, client, *, args=None):
        import contextlib, os, runpy, sys
        environment = {'MAXIMIZER_AWS_REGION': 'eu-central-1', 'MAXIMIZER_AWS_ACCESS_KEY_ID': 'synthetic',
            'MAXIMIZER_AWS_SECRET_ACCESS_KEY': 'synthetic', 'MAXIMIZER_RESULTS_BUCKET': 'demo-results',
            'MAXIMIZER_RESULTS_QUEUE_URL': 'must-not-contact-queue', 'MAXIMIZER_UPLOAD_FUNCTION': 'must-not-invoke'}
        output = io.StringIO()
        def only_s3(service, **kwargs):
            self.assertEqual(service, 's3')
            self.assertEqual(kwargs['config'].retries['total_max_attempts'], 1)
            return client
        with patch.dict(os.environ, environment, clear=True), patch.object(sys, 'argv',
                ['upload_and_fetch.py', '--result-only', *(args or ['category_a', 'callonly001'])]), \
                patch.object(published_client.boto3, 'client', side_effect=only_s3) as factory, \
                patch.object(published_client.time, 'sleep') as sleep, contextlib.redirect_stdout(output):
            runpy.run_path(str(Path(published_client.__file__)), run_name='__main__')
        return factory, sleep, output.getvalue()

    def test_actual_result_only_cli_ignores_upload_and_queue_and_replaces_symlink_privately(self):
        import os, stat
        client = Mock(spec=['head_object', 'get_object'])
        data = b'{"private-result":"do-not-print"}'
        client.get_object.return_value = {'Body': io.BytesIO(data)}
        previous = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            outside = Path(directory) / 'outside.json'; outside.write_bytes(b'unchanged')
            work = Path(directory) / 'work'; work.mkdir(); os.chdir(work)
            try:
                Path('callonly001.json').symlink_to(outside)
                factory, sleep, output = self.run_cli(client)
                factory.assert_called_once(); sleep.assert_not_called()
                client.head_object.assert_called_once_with(Bucket='demo-results', Key='category_a/callonly001.json')
                client.get_object.assert_called_once_with(Bucket='demo-results', Key='category_a/callonly001.json')
                self.assertEqual(Path('callonly001.json').read_bytes(), data)
                self.assertFalse(Path('callonly001.json').is_symlink())
                self.assertEqual(stat.S_IMODE(Path('callonly001.json').stat().st_mode), 0o600)
                self.assertEqual(outside.read_bytes(), b'unchanged')
                self.assertNotIn('do-not-print', output)
            finally: os.chdir(previous)

    def test_result_only_pending404_polls_then_gets_without_any_other_client(self):
        import os
        client = Mock(spec=['head_object', 'get_object'])
        client.head_object.side_effect = [ClientError({'Error': {'Code': '404'}}, 'HeadObject'), {}]
        client.get_object.return_value = {'Body': io.BytesIO(b'{}')}
        previous = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                _, sleep, _ = self.run_cli(client)
                sleep.assert_called_once_with(15)
                self.assertEqual(client.head_object.call_count, 2)
                client.get_object.assert_called_once()
            finally: os.chdir(previous)

    def test_result_only403_fails_immediately_without_printing_error_body(self):
        client = Mock(spec=['head_object', 'get_object'])
        client.head_object.side_effect = ClientError({'Error': {'Code': 'NotFound', 'Message': 'private-error-body'},
            'ResponseMetadata': {'HTTPStatusCode': 403}}, 'HeadObject')
        with self.assertRaisesRegex(RuntimeError, 'HTTP 403') as caught:
            self.run_cli(client)
        self.assertNotIn('private-error-body', str(caught.exception))
        client.get_object.assert_not_called()
        self.assertEqual(client.head_object.call_count, 1)

    def test_result_only_failed_get_preserves_existing_file_and_leaves_no_temp(self):
        import os
        client = Mock(spec=['head_object', 'get_object'])
        client.get_object.side_effect = OSError('private-network-url')
        previous = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                Path('callonly001.json').write_bytes(b'old-owned-file')
                with self.assertRaisesRegex(RuntimeError, 'Result read failed') as caught:
                    self.run_cli(client)
                self.assertNotIn('private-network-url', str(caught.exception))
                self.assertEqual(Path('callonly001.json').read_bytes(), b'old-owned-file')
                self.assertEqual(sorted(p.name for p in Path('.').iterdir()), ['callonly001.json'])
            finally: os.chdir(previous)

    def test_result_only_validation_precedes_sdk_construction(self):
        for args in [('bad/category', 'callonly001'), ('category_a', '../private'),
                ('category_a', 'unsafe\\path'), ('category_a', 'C:drive'), ('category_a', 'line\nend')]:
            with patch.object(published_client.boto3, 'client') as factory:
                with self.assertRaises(SystemExit):
                    published_client.fetch_result_only(*args)
                factory.assert_not_called()


if __name__ == '__main__':
    unittest.main()
