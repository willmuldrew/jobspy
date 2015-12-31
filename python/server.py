import flask
from flask import request

app = flask.Flask(__name__)

@app.route('/jobspy/<run_id>/meta', methods=["POST"])
def post_meta(run_id):
    print run_id, request.json
    return 'OK'

@app.route('/jobspy/<run_id>/output', methods=["POST"])
def post_output(run_id):
    for (t, fd, line) in request.json:
        print run_id, t, fd, line
    return 'OK'

if __name__ == '__main__':
    app.run()
