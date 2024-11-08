import math
import requests
import pytest
import uuid

HOST = "localhost"
PORT = 3000

def test_creating_channel():
    cid = "C-" + uuid.uuid4().hex
    create_channel(cid, "Things", "owner1")

    channel = get_channel(cid)
    assert channel["id"] == cid
    assert channel["topic"] == "Things"
    assert channel["owner"] == "owner1"

    delete_channel(cid)

    channel = get_channel_raw(cid)
    assert channel.status_code == 404

def test_posting_messages():
    cid = "C-" + uuid.uuid4().hex
    create_channel(cid, "Channel 1", "owner1")

    post_message(cid, "author1", "Message 1")
    post_message(cid, "author2", "Message 2")

    messages = get_messages(cid)
    assert len(messages) == 2
    assert messages[0]["author"] == "author1"
    assert messages[0]["text"] == "Message 1"
    assert messages[1]["author"] == "author2"
    assert messages[1]["text"] == "Message 2"

    delete_channel(cid)

def test_getting_messages():
    cid = "C-" + uuid.uuid4().hex
    create_channel(cid, "Channel 1", "owner1")

    post_message(cid, "author1", "Message 1")
    post_message(cid, "author2", "Message 2")
    post_message(cid, "author1", "Message 3")
    post_message(cid, "author2", "Message 4")
    post_message(cid, "author1", "Message 5")

    messages = get_messages(cid)
    assert len(messages) == 5

    third_message_time = messages[2]["timestamp"]

    messages_at = get_messages_start_at(cid, third_message_time)
    assert len(messages_at) == 3
    assert messages_at[0]["author"] == "author1"
    assert messages_at[0]["text"] == "Message 3"
    assert messages_at[1]["author"] == "author2"
    assert messages_at[1]["text"] == "Message 4"
    assert messages_at[2]["author"] == "author1"
    assert messages_at[2]["text"] == "Message 5"

    messages_by = get_messages_by_author(cid, "author1")
    assert len(messages_by) == 3
    assert messages_by[0]["author"] == "author1"
    assert messages_by[0]["text"] == "Message 1"
    assert messages_by[1]["author"] == "author1"
    assert messages_by[1]["text"] == "Message 3"
    assert messages_by[2]["author"] == "author1"
    assert messages_by[2]["text"] == "Message 5"

    messages_by_at = get_messages_by_author_start_at(cid, "author1", third_message_time)
    assert len(messages_by_at) == 2
    assert messages_by_at[0]["author"] == "author1"
    assert messages_by_at[0]["text"] == "Message 3"
    assert messages_by_at[1]["author"] == "author1"
    assert messages_by_at[1]["text"] == "Message 5"

    delete_channel(cid)

def test_membership():
    cid = "C-" + uuid.uuid4().hex
    create_channel(cid, "Channel 1", "owner1")

    register_member(cid, "member1")
    register_member(cid, "member2")

    expected_members = ["owner1", "member1", "member2"]

    members = get_members(cid)
    assert len(members) == 3
    assert "owner1" in expected_members
    assert "member1" in expected_members
    assert "member2" in expected_members

    delete_member(cid, "member1")

    members = get_members(cid)
    assert len(members) == 2
    assert "owner1" in members
    assert "member1" not in members
    assert "member2" in members

    delete_channel(cid)


def create_channel(channel_id, topic, owner):
    response = requests.put(f'http://{HOST}:{PORT}/channels', json={"id": channel_id, "topic": topic, "owner": owner})
    assert response.status_code == 201
    return response.json()

def get_channel_raw(channel_id):
    response = requests.get(f'http://{HOST}:{PORT}/channels/{channel_id}')
    return response

def get_channel(channel_id):
    response = get_channel_raw(channel_id)
    assert response.status_code == 200
    return response.json()

def delete_channel(channel_id):
    response = requests.delete(f'http://{HOST}:{PORT}/channels/{channel_id}')
    assert response.status_code == 204
    return response

def post_message(channel_id, author, text):
    response = requests.put(f'http://{HOST}:{PORT}/channels/{channel_id}/messages', json={"author": author, "text": text})
    assert response.status_code == 201
    return response

def get_messages(channel_id):
    response = requests.get(f'http://{HOST}:{PORT}/channels/{channel_id}/messages')
    assert response.status_code == 200
    return response.json()

def get_messages_start_at(channel_id, start_at):
    response = requests.get(f'http://{HOST}:{PORT}/channels/{channel_id}/messages?startAt={start_at}')
    assert response.status_code == 200
    return response.json()
        
def get_messages_by_author(channel_id, author):
    response = requests.get(f'http://{HOST}:{PORT}/channels/{channel_id}/messages?author={author}')
    assert response.status_code == 200
    return response.json()

def get_messages_by_author_start_at(channel_id, author, start_at):
    response = requests.get(f'http://{HOST}:{PORT}/channels/{channel_id}/messages?author={author}&startAt={start_at}')
    assert response.status_code == 200
    return response.json()

def register_member(channel_id, member):
    response = requests.put(f'http://{HOST}:{PORT}/channels/{channel_id}/members', json={"member": member})
    assert response.status_code == 201
    return response

def get_members(channel_id):
    response = requests.get(f'http://{HOST}:{PORT}/channels/{channel_id}/members')
    assert response.status_code == 200
    return response.json()

def delete_member(channel_id, member):
    response = requests.delete(f'http://{HOST}:{PORT}/channels/{channel_id}/members/{member}')
    assert response.status_code == 204
    return response
