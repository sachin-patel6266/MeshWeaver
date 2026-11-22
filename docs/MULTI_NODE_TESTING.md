\# MeshWeaver Multi-Node Testing Guide



\## 1. Objective



The objective of multi-node testing is to verify communication, peer discovery, task distribution, and remote task execution across multiple MeshWeaver nodes.



\## 2. Test Environment



\- Programming Language: Python

\- Networking: TCP and UDP

\- Communication: Asyncio

\- Peer Discovery: UDP Multicast

\- Task Execution: Remote Workers

\- Security: Mutual TLS and Ed25519 signatures



\## 3. Test Setup



1\. Create a separate identity for each node.

2\. Generate a trusted certificate bundle.

3\. Assign a unique TCP port to each node.

4\. Configure bootstrap addresses.

5\. Start the bootstrap node.

6\. Connect additional worker nodes.



\## 4. Important Test Cases



\### Peer Discovery

Verify that nodes can discover and identify other peers.



\### Remote Task Execution

Submit a task from one node and verify that a remote worker executes it.



\### Worker Failure

Stop a worker and observe whether the system detects its disappearance.



\### Task Timeout

Submit a task with a limited execution timeout and verify that the worker enforces the configured limit.



\### Configuration Validation

Verify that invalid ports, timeout values, and network settings are rejected.



\## 5. Expected Results



\- Nodes should establish authenticated communication.

\- Valid peers should be discovered.

\- Tasks should execute on available workers.

\- Invalid configurations should be rejected.

\- Worker failures should be detected.

\- Task execution should respect timeout policies.



\## 6. Security Considerations



Only trusted peers should be allowed to execute submitted Python tasks.



Cloudpickle is not a security sandbox. Untrusted tasks must not be executed directly with the worker's normal operating-system permissions.



\## 7. Future Improvements



\- Persistent task storage.

\- Process-based task isolation.

\- Automatic certificate rotation.

\- Improved worker recovery.

\- Structured monitoring and logging.

\- Multi-machine performance testing.



\## 8. Conclusion



Multi-node testing is an important step toward validating MeshWeaver as a decentralized asynchronous task broker. Testing on separate physical machines will help identify networking, reliability, and security issues that local tests may not reveal.

