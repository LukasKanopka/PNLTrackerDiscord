from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text, JSON, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from pnl_analyzer.db.base import Base


class Upload(Base):
    __tablename__ = "uploads"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    original_filename: Mapped[str | None] = mapped_column(String(512), nullable=True)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    byte_size: Mapped[int] = mapped_column(Integer, nullable=False)
    mime_type: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # Path to the stored file (relative to UPLOAD_STORE_DIR or absolute).
    storage_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    text_preview: Mapped[str | None] = mapped_column(Text, nullable=True)

    runs: Mapped[list["Run"]] = relationship(back_populates="upload")


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    source_filename: Mapped[str | None] = mapped_column(String(512), nullable=True)
    export_timezone: Mapped[str] = mapped_column(String(64), nullable=False)
    verify_prices: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    upload_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("uploads.id", ondelete="SET NULL"), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="INGESTED")  # INGESTED|EXTRACTED|ANALYZING|DONE|ERROR
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    app_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    settings_snapshot: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    parse_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    extract_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    analyze_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    upload: Mapped[Upload | None] = relationship(back_populates="runs")
    messages: Mapped[list["Message"]] = relationship(back_populates="run", cascade="all, delete-orphan")
    calls: Mapped[list["Call"]] = relationship(back_populates="run", cascade="all, delete-orphan")
    issues: Mapped[list["CallIssue"]] = relationship(back_populates="run", cascade="all, delete-orphan")


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="CASCADE"), index=True)
    message_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    author: Mapped[str] = mapped_column(String(128), nullable=False)
    timestamp_utc: Mapped[str] = mapped_column(String(32), nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)

    run: Mapped[Run] = relationship(back_populates="messages")


class Call(Base):
    __tablename__ = "calls"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="CASCADE"), index=True)

    call_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    author: Mapped[str] = mapped_column(String(128), nullable=False)
    timestamp_utc: Mapped[str] = mapped_column(String(32), nullable=False)
    platform: Mapped[str] = mapped_column(String(32), nullable=False)
    market_intent: Mapped[str] = mapped_column(Text, nullable=False)
    position_direction: Mapped[str] = mapped_column(String(8), nullable=False)
    quoted_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    bet_size_units: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    source_message_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    action: Mapped[str | None] = mapped_column(String(16), nullable=True)
    market_ref: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    extraction_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    evidence: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    run: Mapped[Run] = relationship(back_populates="calls")
    result: Mapped["CallResult | None"] = relationship(back_populates="call", cascade="all, delete-orphan", uselist=False)


class CallResult(Base):
    __tablename__ = "call_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    call_id: Mapped[int] = mapped_column(Integer, ForeignKey("calls.id", ondelete="CASCADE"), unique=True, index=True)

    status: Mapped[str] = mapped_column(String(32), nullable=False)  # OK|UNMATCHED|PENDING|ERROR
    matched_market_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    matched_market_title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    match_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    match_method: Mapped[str | None] = mapped_column(String(32), nullable=True)  # url|ticker|search|llm

    resolved_outcome: Mapped[str | None] = mapped_column(String(8), nullable=True)  # YES|NO
    entry_price_used: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_source: Mapped[str | None] = mapped_column(String(128), nullable=True)
    price_quality: Mapped[str | None] = mapped_column(String(32), nullable=True)  # HISTORICAL|QUOTED|MISSING|APPROXIMATE
    price_ts_utc: Mapped[str | None] = mapped_column(String(32), nullable=True)

    contracts: Mapped[float | None] = mapped_column(Float, nullable=True)
    fees_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    net_pnl_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    roi: Mapped[float | None] = mapped_column(Float, nullable=True)
    debug_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    call: Mapped[Call] = relationship(back_populates="result")


class PriceCache(Base):
    __tablename__ = "price_cache"
    __table_args__ = (UniqueConstraint("platform", "market_id", "side", "minute_ts", name="uq_price_cache_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    platform: Mapped[str] = mapped_column(String(32), nullable=False)
    market_id: Mapped[str] = mapped_column(String(128), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    minute_ts: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str | None] = mapped_column(String(128), nullable=True)


class CallIssue(Base):
    __tablename__ = "call_issues"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="CASCADE"), index=True)
    call_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("calls.id", ondelete="CASCADE"), nullable=True, index=True)

    issue_type: Mapped[str] = mapped_column(String(64), nullable=False)
    details_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    run: Mapped[Run] = relationship(back_populates="issues")
