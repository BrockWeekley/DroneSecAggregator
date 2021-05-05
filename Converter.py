import pandas as pd
import glob
import math
import re


def parse_log(log_row, attribute):

    log_information = [int(s) for s in re.findall(r'\b\d+\b', log_row)]
    goodput = (log_information[2] * .008)
    packets_received = log_information[1]
    acks_received = log_information[0]
    packets_lost = 0

    if attribute == 'flow':
        return flowbytes(goodput)
    elif attribute == 'packet_length':
        return packet_length(goodput, packets_received, acks_received, packets_lost)
    elif attribute == 'subflow':
        return subflow(goodput, packets_received, acks_received, packets_lost)
    elif attribute == 'total_length':
        return subflow(goodput, packets_received, acks_received, packets_lost)
    elif attribute == 'init_win':
        return init_win(goodput, packets_received, acks_received, packets_lost)
    elif attribute == 'pkt_fwd':
        return packets_received
    elif attribute == 'iat_min':
        return iatmin(packets_received)
    elif attribute == 'bwd packets':
        return acks_received
    else:
        return init_win(goodput, packets_received, acks_received, packets_lost)


def flowbytes(goodput):
    return goodput * 125


def subflow(goodput, packets_received, acks_received, packets_lost):
    if (packets_received + acks_received + packets_lost) > 0:
        return goodput/(packets_received + acks_received + packets_lost) * packets_received
    else:
        return -1


def init_win(goodput, packets_received, acks_received, packets_lost):
    if (packets_received + acks_received + packets_lost) > 0:
        return goodput/(packets_received + acks_received + packets_lost)
    else:
        return -1


def iatmin(packets_received):
    if packets_received > 0:
        return math.floor(1/packets_received)
    else:
        return packets_received


def packet_length(goodput, packets_received, acks_received, packets_lost):
    if (packets_received + acks_received + packets_lost) > 0:
        return (goodput/(packets_received + acks_received + packets_lost)) * packets_received
    else:
        return -1


data = [pd.read_csv(filename) for filename in glob.glob('./SPIDER3D/*.csv')]
powder_logs = [pd.read_csv(filename) for filename in glob.glob('./collection/*.log')]

model_data = {
    'Flow Bytes /s': [],
    'BWD Packet Length Std': [],
    'Destination Port': [],
    'Subflow Fwd Bytes': [],
    'Total Length of Fwd Packets': [],
    'Init_Win_bytes_forward': [],
    'act_data_pkt_fwd': [],
    'Fwd IAT Min': [],
    'Bwd Packets /s': [],
    'Average Packet Size': []
}

model_frame = pd.DataFrame(model_data, columns=[
    'Flow Bytes /s',
    'BWD Packet Length Std',
    'Destination Port',
    'Subflow Fwd Bytes',
    'Total Length of Fwd Packets',
    'Init_Win_bytes_forward',
    'act_data_pkt_fwd',
    'Fwd IAT Min',
    'Bwd Packets /s',
    'Average Packet Size'
])

i = 0

for file in data:
    file_frame = pd.DataFrame(file, columns=[
        'Time',
        'PacketsReceived',
        'ACKsReceived',
        'PacketsLost',
        'Goodput(kbit/s)'
    ])
    model_frame['Flow Bytes /s'] = file_frame.apply(lambda column: flowbytes(column['Goodput(kbit/s)']), axis=1)
    model_frame['BWD Packet Length Std'] = file_frame.apply(lambda column: packet_length(
        column['Goodput(kbit/s)'],
        column['PacketsReceived'],
        column['ACKsReceived'],
        column['PacketsLost']
    ), axis=1)
    model_frame['Destination Port'] = 80
    model_frame['Subflow Fwd Bytes'] = file_frame.apply(lambda column: subflow(
        column['Goodput(kbit/s)'],
        column['PacketsReceived'],
        column['ACKsReceived'],
        column['PacketsLost']
    ), axis=1)
    model_frame['Total Length of Fwd Packets'] = file_frame.apply(lambda column: subflow(
        column['Goodput(kbit/s)'],
        column['PacketsReceived'],
        column['ACKsReceived'],
        column['PacketsLost']
    ), axis=1)
    model_frame['Init_Win_bytes_forward'] = file_frame.apply(lambda column: init_win(
        column['Goodput(kbit/s)'],
        column['PacketsReceived'],
        column['ACKsReceived'],
        column['PacketsLost']
    ), axis=1)
    model_frame['act_data_pkt_fwd'] = file_frame.apply(lambda column: column['PacketsReceived'], axis=1)
    model_frame['Fwd IAT Min'] = file_frame.apply(lambda column: iatmin(
        column['PacketsReceived']
    ), axis=1)
    model_frame['Bwd Packets /s'] = file_frame.apply(lambda column: column['ACKsReceived'], axis=1)
    model_frame['Average Packet Size'] = file_frame.apply(lambda column: init_win(
        column['Goodput(kbit/s)'],
        column['PacketsReceived'],
        column['ACKsReceived'],
        column['PacketsLost']
    ), axis=1)

    model_frame.to_csv('./ModelReadyData/model' + str(i) + '.csv', index=False, header=True)
    i += 1

for file in powder_logs:
    file_frame = pd.DataFrame(file, columns=[
        'Log'
    ])

    model_frame['Flow Bytes /s'] = file_frame.apply(lambda column: parse_log(column['Log'], 'flow'), axis=1)
    model_frame['BWD Packet Length Std'] = file_frame.apply(lambda column: parse_log(column['Log'], 'packet_length'), axis=1)
    model_frame['Destination Port'] = 80
    model_frame['Subflow Fwd Bytes'] = file_frame.apply(lambda column: parse_log(column['Log'], 'subflow'), axis=1)
    model_frame['Total Length of Fwd Packets'] = file_frame.apply(lambda column: parse_log(column['Log'], 'total_length'), axis=1)
    model_frame['Init_Win_bytes_forward'] = file_frame.apply(lambda column: parse_log(column['Log'], 'init_win'), axis=1)
    model_frame['act_data_pkt_fwd'] = file_frame.apply(lambda column: parse_log(column['Log'], 'pkt_fwd'), axis=1)
    model_frame['Fwd IAT Min'] = file_frame.apply(lambda column: parse_log(column['Log'], 'iat_min'), axis=1)
    model_frame['Bwd Packets /s'] = file_frame.apply(lambda column: parse_log(column['Log'], 'bwd_packets'), axis=1)
    model_frame['Average Packet Size'] = file_frame.apply(lambda column: parse_log(column['Log'], 'packet_size'), axis=1)

    model_frame.to_csv('./ModelReadyData/model' + str(i) + '.csv', index=False, header=True)
    i += 1
