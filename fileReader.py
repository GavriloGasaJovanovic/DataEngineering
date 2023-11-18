import json

valid_os = {'Android', 'iOS', 'Web'}
valid_amount = {0.99, 1.99, 2.99, 4.99, 9.99}
valid_currency = {'EUR', 'USD'}
valid_type = {'login', 'logout', 'transaction', 'registration'}
event_id_set = set()
user_activity_data = {} #

event_data_error_len = 0
logic_error_len = 0



def is_unix_timestamp(timestamp):
    try:
        timestamp_float = float(timestamp)
        return timestamp_float >= 0
    except ValueError:
        return False

def filter_event_data_function(event_data, event_type):
    if event_data['user_id'] is None:
        return { 'passed': False, 'reason': 'Event data error: Doesn\'t have user id' }
    if event_type == 'registration':
        if event_data['device_os'] not in valid_os:
            return { 'passed': False, 'reason': 'Registration error: Not a valid os device' }
        if event_data['country'] is None or event_data['country'] == "":
            return {'passed': False, 'reason': 'Registration error: Not a valid country'}

    if event_type == 'transaction':
        if event_data['transaction_currency'] not in valid_currency:
            return { 'passed': False, 'reason': 'Transaction error: Not a valid currency'}
        if event_data['transaction_amount'] not in valid_amount:
            return {'passed': False, 'reason': 'Transaction error: Not a valid amount'}

    return { 'passed': True }

def filter_by_logic_function(user_id, event_type):
    if event_type == 'registration':
        if user_id in user_activity_data:
            return { 'passed': False, 'reason': 'Registration error: This user is in data'}
        else:
            user_activity_data[user_id] = 'logout'
    elif event_type == 'login':
        if user_id not in user_activity_data:
            return { 'passed': False, 'reason': 'Login error: This user is not in data'}
        if user_activity_data[user_id] != 'logout':
            return { 'passed': False, 'reason': 'Login error: This user must be logout'}
        user_activity_data[user_id] = 'login'
    elif event_type == 'logout':
        if user_id not in user_activity_data:
            return { 'passed': False, 'reason': 'Logout error: This user is not in data'}
        if user_activity_data[user_id] != 'login':
            return { 'passed': False, 'reason': 'Logout error: This user must be login'}
        user_activity_data[user_id] = 'logout'
    elif event_type == 'transaction':
        if user_id not in user_activity_data:
            return { 'passed': False, 'reason': 'Transaction error: This user is not in data'}
        if user_activity_data[user_id] != 'login':
            return { 'passed': False, 'reason': 'Transaction error: This user must be login'}
    return { 'passed': True }

def filter_event_function(line):
    global event_data_error_len
    global logic_error_len
    # Remove duplicated by id
    if line['event_id'] in event_id_set:
        return { 'passed': False, 'reason': 'Duplicate event_id'}
    # Remove wrong type of event type
    if line['event_type'] not in valid_type:
        return { 'passed': False, 'reason': 'Wrong event_type'}
    if is_unix_timestamp(line['event_timestamp']) is False:
        return { 'passed': False, 'reason': 'Invalid event_timestamp' }
    event_data = line['event_data']
    if event_data is None:
        return { 'passed': False, 'reason': 'event_data is None' }
    event_data_info = filter_event_data_function(event_data, line['event_type'])
    if event_data_info['passed'] is False:
        event_data_error_len += 1
        return event_data_info
    logic_info = filter_by_logic_function(event_data['user_id'], line['event_type'])
    if logic_info['passed'] is False:
        logic_error_len += 1
        return logic_info
    event_id_set.add(line['event_id'])
    return { 'passed': True }

def read_file(file_path):
    result = []
    with open(file_path, 'r') as file:
        for line in file:
            # Parse JSON from each line
            json_obj = json.loads(line)
            result.append(json_obj)
    return result

def parse_data(data):
    valid_data = []
    invalid_data = []
    for line in data:
        line_info = filter_event_function(line)
        if line_info['passed']:
            valid_data.append(line)
        else:
            line['reason'] = line_info['reason']
            invalid_data.append(line)

    return valid_data, invalid_data


file_path = './data/events.jsonl'
all_data = read_file(file_path)
sorted_all_data = sorted(all_data, key= lambda x: x['event_timestamp'])
valid_data, invalid_data = parse_data(sorted_all_data)

# Write the filtered events back to a new JSONL file
filtered_jsonl_file_path = './data/valid_events.jsonl'
with open(filtered_jsonl_file_path, 'w') as filtered_file:
    for event in valid_data:
        # Convert the event back to JSON and write to the file
        filtered_file.write(json.dumps(event) + '\n')

print("Filtered JSONL file created successfully.")

# Write the discarded events to a new JSONL file with reasons
discarded_jsonl_file_path = './data/invalid_events.jsonl'
with open(discarded_jsonl_file_path, 'w') as discarded_file:
    for event in invalid_data:
        # Convert the event back to JSON and write to the file
        discarded_file.write(json.dumps(event) + '\n')

print("Discarded elements JSONL file created successfully.")
print(f'Error_logic: { logic_error_len }, Error_event_data: {event_data_error_len}')