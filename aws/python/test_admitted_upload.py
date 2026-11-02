import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from admitted_upload import AdmittedUploads


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


if __name__ == '__main__':
    unittest.main()
