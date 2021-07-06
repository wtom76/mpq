# MpqS
Is a main solution candidate

Single queue is used to allow producers to add new messages while previously enqueued ones are processed.
There is at least one improvement to be considered - thread pool utilization in message consumption.

# Mpq
Is first and less productive attempt and is left as a backup solution in case of one queue is prohibited for some reason.
Poorer performance is related mainly to producers been blocked while older messages are processed. Can't find a workaround without having shared precreated queue.

note: test code is far from perfection, at least it does it's job
