#!/usr/bin/env python
import flask
from flask import request, abort, render_template
import requests

app = flask.Flask(__name__)

ES_BASE = "http://localhost:9200/jobspy"

META_SEARCH_URL = "{}/jobmeta/_search".format(ES_BASE) 


def _get_job_meta(job_uuid):
    rc = requests.get(ES_BASE + "/jobmeta/{}".format(job_uuid))
    if rc.status_code == 200:
        return rc.json()['_source']
    else:
        return None


def _get_recent_jobs():
    # TODO - need to specify recent
    print META_SEARCH_URL
    return requests.get(META_SEARCH_URL).json()['hits']['hits']


@app.route('/job/<job_uuid>')
def get_job(job_uuid):
    m = _get_job_meta(job_uuid)
    if m is not None:
        return render_template("job.html", jobmeta=m)
    else:
        abort(404)


@app.route("/jobs")
def get_jobs():
    jobmetas = _get_recent_jobs()
    return render_template("jobs.html", jobmetas=jobmetas)


if __name__ == '__main__':
    app.run()
