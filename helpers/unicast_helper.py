def pack_message(message_list):
    return ",".join([str(x) for x in message_list])


def unpack_message(message):
    message = message.decode('utf-8')
    sender, message_id, is_ack, vector_str, message = message.split(',', 4)

    sender = int(sender)
    message_id = int(message_id)
    timestamp = parse_vector_timestamp(vector_str)
    is_ack = is_ack in ['True', 'true', '1']

    return [sender, message_id, is_ack, timestamp, message]


def parse_vector_timestamp(vector_str):
    return [int(x) for x in vector_str.split(';')]


def stringify_vector_timestamp(vector):
    return ';'.join([str(x) for x in vector])