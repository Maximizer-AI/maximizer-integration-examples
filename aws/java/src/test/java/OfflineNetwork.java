// Test-only guard: no SDK, HTTP client or DNS request may leave the test JVM.
@SuppressWarnings("removal")
final class OfflineNetwork extends SecurityManager {
    static SecurityManager previous;
    static void install() { previous=System.getSecurityManager(); System.setSecurityManager(new OfflineNetwork()); }
    static void restore() { System.setSecurityManager(previous); }
    @Override public void checkPermission(java.security.Permission permission) { }
    @Override public void checkConnect(String host,int port) { throw new AssertionError("Network access is forbidden in offline tests"); }
    @Override public void checkListen(int port) { throw new AssertionError("Listening sockets are forbidden in offline tests"); }
}
