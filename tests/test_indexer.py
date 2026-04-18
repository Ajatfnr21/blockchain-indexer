"""Tests for Blockchain Indexer"""

import pytest
import asyncio
from fastapi.testclient import TestClient

from indexer.main import app, db, EVMIndexer, EventDecoder, IndexedEvent

client = TestClient(app)


class TestHealth:
    def test_health_check(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_info(self):
        response = client.get("/")
        assert response.status_code == 200
        assert "Blockchain Indexer" in response.json()["name"]


class TestEventDecoder:
    def test_decode_transfer(self):
        topics = [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "0x0000000000000000000000001234567890123456789012345678901234567890",
            "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcdef"
        ]
        data = "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000"
        result = EventDecoder.decode_transfer(data, topics)
        assert "from" in result
        assert "to" in result
        assert result["from"] == "0x1234567890123456789012345678901234567890"


class TestAPI:
    def test_get_stats(self):
        response = client.get("/stats")
        assert response.status_code == 200
        data = response.json()
        assert "total_events" in data

    def test_query_events_empty(self):
        response = client.get("/events?network=ethereum&limit=10")
        assert response.status_code == 200
        data = response.json()
        assert "events" in data

    def test_start_backfill_validation(self):
        response = client.post("/index/ethereum/backfill?start_block=100&end_block=200")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "started"
        assert data["network"] == "ethereum"

    def test_invalid_network(self):
        response = client.post("/index/invalid/backfill?start_block=100&end_block=200")
        assert response.status_code == 400
