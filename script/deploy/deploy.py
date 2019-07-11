#!/bin/bash
import os
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
env_dict = os.environ

host = "https://{}:8443".format(env_dict['AZKABAN_HOST'])


def login(user, passwd):
    r = requests.post(
        host + "/?action=login",
        data={
            "username": user,
            "password": passwd
        },
        verify=False)
    session_id = r.json().get('session.id')
    return session_id


def create_proj(project, des, session_id):
    r = requests.post(
        host + "/manager?action=create",
        data={
            "session.id": session_id,
            "name": project,
            "description": des
        },
        verify=False)
    print(r.json())


def delete_proj(project, session_id):
    r = requests.get(
        host + "/manager",
        params={
            "session.id": session_id,
            "project": project,
            "delete": "true"
        },
        verify=False)


def upload(file, session_id):
    r = requests.post(
        host + "/manager",
        data={
            "session.id": session_id,
            "project": project,
            "ajax": "upload"
        },
        files={"file": (file, open(file, "rb"), "application/zip")},
        verify=False)
    print(r.json())


def schedule(project, flow, cron_expression, session_id):
    r = requests.post(
        host + "/schedule",
        data={
            "session.id": session_id,
            "projectName": project,
            "flow": flow,
            "ajax": "scheduleCronFlow",
            "cronExpression": cron_expression
        },
        verify=False)
    print(r.json())


if __name__ == '__main__':
    session_id = login(env_dict['AZKABAN_USER'], env_dict['AZKABAN_PASS'])
    project = "data-etl-sloth"
    delete_proj(project, session_id)
    create_proj(project, "RT", session_id)
    upload("flow.zip", session_id)
    d = {"az_dispatch":"0 15 0 ? * *","meta_data":"0 5 0 ? * *","task_monitoring":"0 30 0,5,7 ? * *","sloth_flow_rds_retry":"0 0 5 ? * *"}
    for key, value in d.items():
        schedule(project, key, value, session_id)
