#Reliable Multicast Chat

## Usage

./reliable_multicast_chat [process ID] [delay time (in seconds)] [drop rate (0<=P<1)]

Execute `reliable_multicast_chat` in `./bin` folder.

## Configurations

Modify `config.py` in `reliable_mulicast_chat` directory accordingly. 
`config['hosts']` is a list of (IP address, port).
Ordering can be either 'casual' or 'total'.

## Algorithms

- **Casual Ordering**: When a process receives a multicasted message, it pushes the received message to a buffer
queue with a vector timestamp from the sender. And then `update_holdback_queue_casual()` takes care of updating the buffer
and actually delivering the message to the process. It compares the timestamps of the buffered messages with the process's timestamp to determine which messages should be kept in the buffer or be removed and delivered to the process.

- **Total Ordering**: Process 0 is always designated as the sequencer. When a process multicasts a message, other processes will hold that message in a buffer until they receive a marker message from the sequencer indicating the order of that message. When the sequencer receives a message, it increments an internal counter and assigns that number as the message's order number which will then be multicasted to all other processes.


## Requirement

* Python3
