# tests/test_jobs.py
import json


def test_init_job(client, mock_dependencies):
    """Test POST /job/init"""
    payload = {
        "name": "test-export",
        "job_type": "export_pdf",
        "user_email": "admin@example.com",
        "track_async_children": True,
    }

    mock_dependencies["init_job"].return_value = "123"

    response = client.post("/job/init", json=payload)

    # Assertions
    assert response.status_code == 200
    assert response.json == {"job_id": "123"}

    mock_dependencies["init_job"].assert_called_once_with(
        "test-export",
        "export_pdf",
        get_rabbit=mock_dependencies["rabbit"],
        user_email="admin@example.com",
        parent_id=None,
        id_of_document_job_was_initiated_for=None,
        track_async_children=True,
    )


def test_start_job(client, mock_dependencies):
    """Test POST /job/start/<id>"""
    job_id = "job_123"
    response = client.post(f"/job/start/{job_id}")

    assert response.status_code == 200
    mock_dependencies["start_job"].assert_called_once_with(
        job_id, get_rabbit=mock_dependencies["rabbit"]
    )


def test_add_document_to_job(client, mock_dependencies):
    """Test POST /job/add_document/<id>"""
    job_id = "job_123"
    payload = {"id_of_document_job_was_initiated_for": "doc_555"}

    response = client.post(f"/job/add_document/{job_id}", json=payload)

    assert response.status_code == 200
    mock_dependencies["add_doc"].assert_called_once_with(
        job_id, "doc_555", get_rabbit=mock_dependencies["rabbit"]
    )


def test_finish_job(client, mock_dependencies):
    """Test POST /job/finish/<id>"""
    job_id = "job_123"
    response = client.post(f"/job/finish/{job_id}")

    assert response.status_code == 200
    mock_dependencies["finish_job"].assert_called_once_with(
        job_id, get_rabbit=mock_dependencies["rabbit"]
    )


def test_fail_job(client, mock_dependencies):
    """Test POST /job/fail/<id>"""
    job_id = "job_123"
    payload = {"exception_message": "Something exploded"}

    response = client.post(f"/job/fail/{job_id}", json=payload)

    assert response.status_code == 200
    mock_dependencies["fail_job"].assert_called_once_with(
        job_id, "Something exploded", get_rabbit=mock_dependencies["rabbit"]
    )


def test_warn_job(client, mock_dependencies):
    """Test POST /job/warn/<id>"""
    job_id = "job_123"
    payload = {"info_message": "Process slow but successful"}

    response = client.post(f"/job/warn/{job_id}", json=payload)

    assert response.status_code == 200
    mock_dependencies["warn_job"].assert_called_once_with(
        job_id,
        info_message="Process slow but successful",
        get_rabbit=mock_dependencies["rabbit"],
    )
