def pack_message(message_list):
    return ",".join([str(x) for x in message_list])

def unpack_message(message):
    message = message.decode('utf-8')
    # return (sender, message_id, is_ack, message)
    return message.split(',', 4)

def parse_vector_timestamp(vector_str):
    return [int(x) for x in vector_str.split(';')]

def stringify_vector_timestamp(vector):
    return ';'.join([str(x) for x in vector])