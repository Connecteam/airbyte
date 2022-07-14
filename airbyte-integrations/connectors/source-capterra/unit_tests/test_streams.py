#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
from source_capterra.source import CapterraStream


@pytest.fixture
def patch_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(CapterraStream, "path", "v0/example_endpoint")
    mocker.patch.object(CapterraStream, "primary_key", "test_primary_key")
    mocker.patch.object(CapterraStream, "__abstractmethods__", set())


def test_request_params(patch_base_class):
    stream = CapterraStream()
    inputs = {"stream_slice": {'date': '2022-06-30'}, "stream_state": None, "next_page_token": None}
    expected_params = {'start_date': '2022-06-30', 'end_date': '2022-06-30'}
    assert stream.request_params(**inputs) == expected_params


def test_next_page_token(patch_base_class):
    stream = CapterraStream()
    m = MagicMock()
    m.json.return_value = {'data': [], 'scroll_id': '1234567'}
    inputs = {"response": m}
    expected_token = {'scroll_id': '1234567'}
    assert stream.next_page_token(**inputs) == expected_token


def test_parse_response(patch_base_class):
    stream = CapterraStream()
    m = MagicMock()
    m.json.return_value = {'data': [{
      "date_of_report": "2022-06-01",
      "vendor_name": "Connecteam",
      "product_name": "Connecteam",
      "category": "Time Clock",
      "avg_cpc": 20.63,
      "avg_position": 2.84,
      "clicks": 32,
      "conversions": 4,
      "conversion_rate": 12.5,
      "cost": 660,
      "cpl": 165,
      "channel": "Capterra",
      "country": "United States",
      "email": "capterra@connecteam.com",
      "vendor_id": 2107717
    }]}
    inputs = {"response": m}
    expected_parsed_object = {
      'capterra_pk': '2022-06-01_Connecteam_Connecteam_Time Clock',
      "date_of_report": "2022-06-01",
      "vendor_name": "Connecteam",
      "product_name": "Connecteam",
      "category": "Time Clock",
      "avg_cpc": 20.63,
      "avg_position": 2.84,
      "clicks": 32,
      "conversions": 4,
      "conversion_rate": 12.5,
      "cost": 660,
      "cpl": 165,
      "channel": "Capterra",
      "country": "United States",
      "email": "capterra@connecteam.com",
      "vendor_id": 2107717
    }
    assert next(stream.parse_response(**inputs)) == expected_parsed_object


def test_request_headers(patch_base_class):
    stream = CapterraStream()
    # TODO: replace this with your input parameters
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    # TODO: replace this with your expected request headers
    expected_headers = {}
    assert stream.request_headers(**inputs) == expected_headers


def test_http_method(patch_base_class):
    stream = CapterraStream()
    # TODO: replace this with your expected http request method
    expected_method = "GET"
    assert stream.http_method == expected_method


@pytest.mark.parametrize(
    ("http_status", "should_retry"),
    [
        (HTTPStatus.OK, False),
        (HTTPStatus.BAD_REQUEST, False),
        (HTTPStatus.TOO_MANY_REQUESTS, True),
        (HTTPStatus.INTERNAL_SERVER_ERROR, True),
    ],
)
def test_should_retry(patch_base_class, http_status, should_retry):
    response_mock = MagicMock()
    response_mock.status_code = http_status
    stream = CapterraStream()
    assert stream.should_retry(response_mock) == should_retry


def test_backoff_time(patch_base_class):
    response_mock = MagicMock()
    stream = CapterraStream()
    expected_backoff_time = None
    assert stream.backoff_time(response_mock) == expected_backoff_time
