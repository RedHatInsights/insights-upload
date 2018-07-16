import json
import time

from kiel import clients, exc

MQ = 'kafka:29092'

mqp = clients.Producer([MQ])
mqc = clients.SingleConsumer([MQ])


def main():
    try:
        while True:
            time.sleep(10)

            mqc.connect()
            mqp.connect()

            try:
                msgs = yield mqc.consume('testareno')
                if msgs:
                    for msg in msgs:
                        print(msg)
                    mqp.produce('uploadvalidation', json.dumps({'validation': 'Success'}))
            except exc.NoBrokersError:
                continue
    except KeyboardInterrupt:
        print('Exiting: Keyboard Interrupt')


if __name__ == '__main__':
    main()
