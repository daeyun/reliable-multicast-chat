def pack_message(message_list):
    return ",".join([str(x) for x in message_list])

def unpack_message(message):
    message = message.decode('utf-8')
    # return (sender, message_id, is_ack, message)
    return message.split(',', 3)
