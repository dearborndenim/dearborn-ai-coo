"""
Database models for COO module.
Uses SQLAlchemy with PostgreSQL.
"""
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, Float,
    Boolean, DateTime, JSON, Index, ForeignKey, Enum as SQLEnum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from contextlib import contextmanager
import enum

from .config import get_settings

settings = get_settings()

# Schema name for COO module
COO_SCHEMA = "coo"

# Create engine with connection pooling
engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=300
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# Enums
class MaterialType(str, enum.Enum):
    FABRIC = "fabric"
    TRIM = "trim"
    PACKAGING = "packaging"
    OTHER = "other"


class ProductionStage(str, enum.Enum):
    CUTTING = "cutting"
    SEWING = "sewing"
    FINISHING = "finishing"
    QC = "qc"
    COMPLETE = "complete"


class AlertSeverity(str, enum.Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


# ============================================================================
# RAW MATERIALS
# ============================================================================

class Supplier(Base):
    """Suppliers for raw materials."""
    __tablename__ = "suppliers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    contact_name = Column(String(255))
    email = Column(String(255))
    phone = Column(String(50))
    address = Column(Text)

    # Lead times
    avg_lead_time_days = Column(Integer, default=14)
    min_order_quantity = Column(Float)
    min_order_unit = Column(String(50))  # 'yards', 'pieces', 'rolls'

    # Payment terms
    payment_terms = Column(String(100))  # 'Net 30', 'COD', etc.

    notes = Column(Text)
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relations
    materials = relationship("RawMaterial", back_populates="supplier")

    __table_args__ = (
        Index('ix_suppliers_name', 'name'),
        {'schema': COO_SCHEMA}
    )


class RawMaterial(Base):
    """Raw materials inventory (fabric, trim, etc.)."""
    __tablename__ = "raw_materials"

    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String(100), unique=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)

    material_type = Column(SQLEnum(MaterialType), default=MaterialType.FABRIC, index=True)

    # For fabric
    fabric_type = Column(String(100))  # 'Selvedge Denim', 'Stretch Denim', etc.
    weight_oz = Column(Float)  # oz per yard
    width_inches = Column(Float)  # fabric width
    color = Column(String(100))

    # Inventory
    quantity_on_hand = Column(Float, default=0)
    quantity_reserved = Column(Float, default=0)  # Reserved for production
    quantity_available = Column(Float, default=0)  # on_hand - reserved
    unit = Column(String(50), default="yards")  # 'yards', 'pieces', 'spools'

    # Thresholds
    reorder_point = Column(Float, default=50)
    reorder_quantity = Column(Float, default=100)

    # Cost
    cost_per_unit = Column(Float, default=0)
    last_cost_update = Column(DateTime)

    # Supplier
    supplier_id = Column(Integer, ForeignKey(f"{COO_SCHEMA}.suppliers.id"))
    supplier = relationship("Supplier", back_populates="materials")
    supplier_sku = Column(String(100))

    # Status
    is_active = Column(Boolean, default=True)
    is_low_stock = Column(Boolean, default=False, index=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('ix_raw_materials_type_stock', 'material_type', 'is_low_stock'),
        {'schema': COO_SCHEMA}
    )


class MaterialTransaction(Base):
    """Track material movements (receiving, usage, adjustments)."""
    __tablename__ = "material_transactions"

    id = Column(Integer, primary_key=True, index=True)
    material_id = Column(Integer, ForeignKey(f"{COO_SCHEMA}.raw_materials.id"), nullable=False)

    transaction_type = Column(String(50), nullable=False, index=True)  # 'receive', 'use', 'adjust', 'return'
    quantity = Column(Float, nullable=False)  # Positive for in, negative for out
    unit = Column(String(50))

    # Reference
    reference_type = Column(String(50))  # 'purchase_order', 'production_run', 'adjustment'
    reference_id = Column(String(100))

    # Cost tracking
    unit_cost = Column(Float)
    total_cost = Column(Float)

    notes = Column(Text)
    created_by = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index('ix_material_txn_material_date', 'material_id', 'created_at'),
        {'schema': COO_SCHEMA}
    )


# ============================================================================
# WORK IN PROGRESS (WIP)
# ============================================================================

class ProductionRun(Base):
    """A production run/batch of products."""
    __tablename__ = "production_runs"

    id = Column(Integer, primary_key=True, index=True)
    run_number = Column(String(50), unique=True, index=True)  # e.g., 'PR-2026-001'

    # Product info
    product_name = Column(String(255), nullable=False)
    product_sku = Column(String(100))
    shopify_product_id = Column(String(100))

    # Quantities
    quantity_planned = Column(Integer, nullable=False)
    quantity_started = Column(Integer, default=0)
    quantity_completed = Column(Integer, default=0)
    quantity_defective = Column(Integer, default=0)

    # Current stage
    current_stage = Column(SQLEnum(ProductionStage), default=ProductionStage.CUTTING, index=True)

    # Dates
    planned_start_date = Column(DateTime)
    actual_start_date = Column(DateTime)
    planned_end_date = Column(DateTime)
    actual_end_date = Column(DateTime)

    # Materials used (JSON list of {material_id, quantity})
    materials_allocated = Column(JSON)

    # Priority and notes
    priority = Column(Integer, default=5)  # 1=highest, 10=lowest
    notes = Column(Text)

    # Status
    status = Column(String(50), default="planned", index=True)  # 'planned', 'in_progress', 'completed', 'cancelled'

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relations
    stage_logs = relationship("ProductionStageLog", back_populates="production_run")

    __table_args__ = (
        Index('ix_production_runs_status_stage', 'status', 'current_stage'),
        {'schema': COO_SCHEMA}
    )


class ProductionStageLog(Base):
    """Track when items move through production stages."""
    __tablename__ = "production_stage_logs"

    id = Column(Integer, primary_key=True, index=True)
    production_run_id = Column(Integer, ForeignKey(f"{COO_SCHEMA}.production_runs.id"), nullable=False)
    production_run = relationship("ProductionRun", back_populates="stage_logs")

    stage = Column(SQLEnum(ProductionStage), nullable=False)
    quantity = Column(Integer, nullable=False)

    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)

    worker = Column(String(100))
    notes = Column(Text)

    __table_args__ = (
        Index('ix_stage_logs_run_stage', 'production_run_id', 'stage'),
        {'schema': COO_SCHEMA}
    )


# ============================================================================
# FINISHED GOODS (Synced from Shopify)
# ============================================================================

class FinishedGoodsInventory(Base):
    """Finished goods inventory (synced from Shopify)."""
    __tablename__ = "finished_goods_inventory"

    id = Column(Integer, primary_key=True, index=True)

    # Shopify identifiers
    shopify_product_id = Column(String(100), index=True)
    shopify_variant_id = Column(String(100), unique=True, index=True)
    shopify_inventory_item_id = Column(String(100))

    # Product info
    product_title = Column(String(255))
    variant_title = Column(String(255))
    sku = Column(String(100), index=True)

    # Inventory levels
    quantity_on_hand = Column(Integer, default=0)
    quantity_committed = Column(Integer, default=0)  # Reserved for orders
    quantity_available = Column(Integer, default=0)

    # Thresholds
    reorder_point = Column(Integer, default=10)
    is_low_stock = Column(Boolean, default=False, index=True)

    # Location (if using multiple warehouses)
    location_id = Column(String(100))
    location_name = Column(String(255))

    # Sync info
    last_synced_at = Column(DateTime, index=True)
    sync_source = Column(String(50), default="shopify")

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('ix_finished_goods_low_stock', 'is_low_stock', 'quantity_available'),
        {'schema': COO_SCHEMA}
    )


# ============================================================================
# SHOPIFY AUTH
# ============================================================================

class ShopifyAuth(Base):
    """Stored Shopify OAuth tokens."""
    __tablename__ = "shopify_auth"

    id = Column(Integer, primary_key=True, index=True)
    store = Column(String(255), unique=True, nullable=False)
    access_token = Column(String(500), nullable=False)
    scope = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = ({'schema': COO_SCHEMA},)


# ============================================================================
# ALERTS AND EVENTS
# ============================================================================

class COOAlert(Base):
    """Alerts generated by COO module."""
    __tablename__ = "coo_alerts"

    id = Column(Integer, primary_key=True, index=True)

    severity = Column(SQLEnum(AlertSeverity), default=AlertSeverity.WARNING, index=True)
    category = Column(String(50), nullable=False, index=True)  # 'inventory', 'production', 'supplier'

    title = Column(String(255), nullable=False)
    message = Column(Text, nullable=False)

    # Related entity
    related_type = Column(String(50))  # 'raw_material', 'production_run', 'finished_goods'
    related_id = Column(Integer)

    # Action
    suggested_action = Column(Text)

    # Status
    is_read = Column(Boolean, default=False, index=True)
    is_resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index('ix_coo_alerts_category_resolved', 'category', 'is_resolved'),
        {'schema': COO_SCHEMA}
    )


class COOEvent(Base):
    """Events for inter-module communication."""
    __tablename__ = "coo_events"

    id = Column(Integer, primary_key=True, index=True)

    direction = Column(String(20), nullable=False, index=True)  # 'inbound', 'outbound'
    other_module = Column(String(50), nullable=False)

    event_type = Column(String(100), nullable=False, index=True)
    payload = Column(JSON)

    status = Column(String(50), default="pending", index=True)
    processed_at = Column(DateTime)
    error_message = Column(Text)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index('ix_coo_events_type_status', 'event_type', 'status'),
        {'schema': COO_SCHEMA}
    )


# ============================================================================
# DATABASE HELPERS
# ============================================================================

@contextmanager
def get_db():
    """Context manager for database sessions."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def init_db():
    """Create schema and all tables."""
    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {COO_SCHEMA}"))
        conn.commit()
    Base.metadata.create_all(bind=engine)


def get_db_session():
    """Dependency for FastAPI routes."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
