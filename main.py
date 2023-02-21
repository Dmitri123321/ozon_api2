from libb.app import App
from libb.functions import make_index
from libb.process_func import process
from libb.threated_rabbit import ReconnectingRabbit


def main(app):
    try:
        make_index(app)
        ReconnectingRabbit(app, process).run()
    except:
        app.sms(f"{app.config['bot_name'], app.config['version'], app.current_node} has been stoped with an error")
        app.sms(files=['info.log', 'warning.log', 'error.log'])


if __name__ == '__main__':
    main(App())
