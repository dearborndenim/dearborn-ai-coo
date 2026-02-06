"""
Dearborn AI COO - Operations Module

Handles inventory management, production tracking, and supply chain operations.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from .config import get_settings
from .db import (
    init_db, get_db_session, SessionLocal,
    RawMaterial, MaterialType, MaterialTransaction,
    Supplier, ProductionRun, ProductionStage, ProductionStageLog,
    FinishedGoodsInventory, COOAlert, AlertSeverity, COOEvent,
    ShopifyAuth, PurchaseOrder, PurchaseOrderItem, POStatus
)
from .event_bus import (
    event_bus, publish_inventory_low, publish_inventory_critical,
    publish_inventory_restocked, publish_production_complete,
    publish_production_costs, publish_po_approval_needed
)

settings = get_settings()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup
    logger.info("=" * 60)
    logger.info("Dearborn AI COO Starting")
    logger.info("=" * 60)

    # Initialize database
    try:
        init_db()
        logger.info("Database initialized (schema: coo)")
    except Exception as e:
        logger.error(f"Database init failed: {e}")

    # Check event bus and start listener
    if event_bus.is_connected():
        logger.info("Event bus connected to Redis")
        event_bus.start_listener()
        logger.info("Event bus listener started")
    else:
        logger.warning("Event bus not connected (HTTP fallback enabled)")

    logger.info("=" * 60)

    yield

    # Shutdown
    event_bus.disconnect()
    logger.info("COO module shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Dearborn AI COO",
    description="""
    Operations module for Dearborn Denim & Apparel.

    ## Features
    - **Raw Materials**: Track fabric, trim, and other materials inventory
    - **Production**: Manage production runs and WIP tracking
    - **Finished Goods**: Sync inventory from Shopify
    - **Alerts**: Low stock and production alerts
    - **Event Bus**: Inter-module communication
    """,
    version="1.0.0",
    lifespan=lifespan
)

# CORS
def _get_allowed_origins():
    env = os.getenv("ALLOWED_ORIGINS", "")
    if env:
        return [o.strip() for o in env.split(",")]
    return []

app.add_middleware(
    CORSMiddleware,
    allow_origins=_get_allowed_origins(),
    allow_origin_regex=r"https://.*\.up\.railway\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class RawMaterialCreate(BaseModel):
    sku: str
    name: str
    description: Optional[str] = None
    material_type: str = "fabric"
    fabric_type: Optional[str] = None
    weight_oz: Optional[float] = None
    width_inches: Optional[float] = None
    color: Optional[str] = None
    quantity_on_hand: float = 0
    unit: str = "yards"
    reorder_point: float = 50
    reorder_quantity: float = 100
    cost_per_unit: float = 0
    supplier_id: Optional[int] = None


class RawMaterialUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    quantity_on_hand: Optional[float] = None
    reorder_point: Optional[float] = None
    reorder_quantity: Optional[float] = None
    cost_per_unit: Optional[float] = None


class MaterialTransactionCreate(BaseModel):
    material_id: int
    transaction_type: str  # 'receive', 'use', 'adjust'
    quantity: float
    unit_cost: Optional[float] = None
    reference_type: Optional[str] = None
    reference_id: Optional[str] = None
    notes: Optional[str] = None


class ProductionRunCreate(BaseModel):
    product_name: str
    product_sku: Optional[str] = None
    shopify_product_id: Optional[str] = None
    quantity_planned: int
    planned_start_date: Optional[datetime] = None
    planned_end_date: Optional[datetime] = None
    priority: int = 5
    notes: Optional[str] = None


class ProductionStageUpdate(BaseModel):
    stage: str  # 'cutting', 'sewing', 'finishing', 'qc', 'complete'
    quantity: int
    worker: Optional[str] = None
    notes: Optional[str] = None


class SupplierCreate(BaseModel):
    name: str
    contact_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    avg_lead_time_days: int = 14
    payment_terms: Optional[str] = None


class POItemCreate(BaseModel):
    material_id: int
    quantity: float
    unit_cost: float
    notes: Optional[str] = None


class PurchaseOrderCreate(BaseModel):
    supplier_id: int
    items: List[POItemCreate]
    shipping_cost: float = 0
    expected_delivery: Optional[datetime] = None
    notes: Optional[str] = None
    source: str = "manual"
    source_reference: Optional[str] = None


class MaterialReservation(BaseModel):
    material_id: int
    quantity: float
    production_run_id: int


# ============================================================================
# HEALTH ENDPOINTS
# ============================================================================

@app.get("/health", tags=["Health"])
async def health_check():
    """Basic health check."""
    return {
        "status": "healthy",
        "module": "coo",
        "version": "1.0.0"
    }


@app.get("/health/detailed", tags=["Health"])
async def detailed_health_check(db: Session = Depends(get_db_session)):
    """Detailed health check including database and Redis."""
    # Check database
    db_status = "healthy"
    try:
        db.execute("SELECT 1")
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"

    return {
        "status": "healthy",
        "module": "coo",
        "database": db_status,
        "redis": "connected" if event_bus.is_connected() else "disconnected",
        "timestamp": datetime.utcnow().isoformat()
    }


# ============================================================================
# SHOPIFY AUTH ENDPOINTS
# ============================================================================

SHOPIFY_SCOPES = ["read_products", "read_inventory"]


def get_shopify_token(db: Session) -> Optional[str]:
    """Get stored Shopify access token."""
    auth = db.query(ShopifyAuth).filter(ShopifyAuth.store == settings.shopify_store).first()
    if auth:
        return auth.access_token
    # Fallback to env var if set
    return settings.shopify_access_token if settings.shopify_access_token else None


@app.get("/coo/auth/shopify", tags=["Auth"])
async def shopify_auth_start(request: Request):
    """Start Shopify OAuth flow."""
    if not settings.shopify_client_id:
        raise HTTPException(status_code=503, detail="Shopify client ID not configured")

    # Generate state for CSRF protection
    import secrets
    state = secrets.token_urlsafe(32)

    # Build redirect URI from request
    redirect_uri = str(request.url).replace("/coo/auth/shopify", "/coo/auth/shopify/callback")
    if "localhost" not in redirect_uri:
        redirect_uri = redirect_uri.replace("http://", "https://")

    params = {
        "client_id": settings.shopify_client_id,
        "scope": ",".join(SHOPIFY_SCOPES),
        "redirect_uri": redirect_uri,
        "state": state
    }
    query = "&".join(f"{k}={v}" for k, v in params.items())
    auth_url = f"https://{settings.shopify_store}/admin/oauth/authorize?{query}"

    return {"auth_url": auth_url, "state": state}


@app.get("/coo/auth/shopify/callback", tags=["Auth"])
async def shopify_auth_callback(
    code: str = Query(...),
    state: str = Query(None),
    db: Session = Depends(get_db_session)
):
    """Handle Shopify OAuth callback."""
    if not settings.shopify_client_id or not settings.shopify_client_secret:
        raise HTTPException(status_code=503, detail="Shopify credentials not configured")

    try:
        import httpx

        # Exchange code for access token
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"https://{settings.shopify_store}/admin/oauth/access_token",
                json={
                    "client_id": settings.shopify_client_id,
                    "client_secret": settings.shopify_client_secret,
                    "code": code
                },
                timeout=30.0
            )

        if response.status_code != 200:
            raise HTTPException(status_code=400, detail=f"Token exchange failed: {response.text}")

        token_data = response.json()
        access_token = token_data.get("access_token")
        scope = token_data.get("scope", "")

        # Store token in database
        existing = db.query(ShopifyAuth).filter(ShopifyAuth.store == settings.shopify_store).first()
        if existing:
            existing.access_token = access_token
            existing.scope = scope
            existing.updated_at = datetime.utcnow()
        else:
            new_auth = ShopifyAuth(
                store=settings.shopify_store,
                access_token=access_token,
                scope=scope
            )
            db.add(new_auth)

        db.commit()

        return {
            "success": True,
            "message": "Shopify connected successfully",
            "scope": scope
        }

    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=f"Request failed: {str(e)}")


@app.get("/coo/auth/shopify/status", tags=["Auth"])
async def shopify_auth_status(db: Session = Depends(get_db_session)):
    """Check Shopify authentication status."""
    token = get_shopify_token(db)
    return {
        "connected": token is not None,
        "store": settings.shopify_store,
        "has_client_id": bool(settings.shopify_client_id)
    }


# ============================================================================
# RAW MATERIALS ENDPOINTS
# ============================================================================

@app.get("/coo/materials", tags=["Raw Materials"])
async def list_materials(
    material_type: Optional[str] = None,
    low_stock_only: bool = False,
    limit: int = Query(default=100, le=500),
    db: Session = Depends(get_db_session)
):
    """List raw materials inventory."""
    query = db.query(RawMaterial).filter(RawMaterial.is_active == True)

    if material_type:
        query = query.filter(RawMaterial.material_type == material_type)
    if low_stock_only:
        query = query.filter(RawMaterial.is_low_stock == True)

    materials = query.order_by(RawMaterial.name).limit(limit).all()

    return [
        {
            "id": m.id,
            "sku": m.sku,
            "name": m.name,
            "material_type": m.material_type.value if m.material_type else None,
            "quantity_on_hand": m.quantity_on_hand,
            "quantity_available": m.quantity_available,
            "unit": m.unit,
            "reorder_point": m.reorder_point,
            "is_low_stock": m.is_low_stock,
            "cost_per_unit": m.cost_per_unit
        }
        for m in materials
    ]


@app.post("/coo/materials", tags=["Raw Materials"])
async def create_material(
    material: RawMaterialCreate,
    db: Session = Depends(get_db_session)
):
    """Create a new raw material."""
    # Check for duplicate SKU
    existing = db.query(RawMaterial).filter(RawMaterial.sku == material.sku).first()
    if existing:
        raise HTTPException(status_code=400, detail=f"Material with SKU {material.sku} already exists")

    db_material = RawMaterial(
        sku=material.sku,
        name=material.name,
        description=material.description,
        material_type=MaterialType(material.material_type) if material.material_type else MaterialType.FABRIC,
        fabric_type=material.fabric_type,
        weight_oz=material.weight_oz,
        width_inches=material.width_inches,
        color=material.color,
        quantity_on_hand=material.quantity_on_hand,
        quantity_available=material.quantity_on_hand,
        unit=material.unit,
        reorder_point=material.reorder_point,
        reorder_quantity=material.reorder_quantity,
        cost_per_unit=material.cost_per_unit,
        supplier_id=material.supplier_id,
        is_low_stock=material.quantity_on_hand <= material.reorder_point
    )

    db.add(db_material)
    db.commit()
    db.refresh(db_material)

    return {"id": db_material.id, "sku": db_material.sku, "name": db_material.name}


@app.get("/coo/materials/{material_id}", tags=["Raw Materials"])
async def get_material(material_id: int, db: Session = Depends(get_db_session)):
    """Get a specific raw material."""
    material = db.query(RawMaterial).filter(RawMaterial.id == material_id).first()
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")

    return {
        "id": material.id,
        "sku": material.sku,
        "name": material.name,
        "description": material.description,
        "material_type": material.material_type.value if material.material_type else None,
        "fabric_type": material.fabric_type,
        "weight_oz": material.weight_oz,
        "width_inches": material.width_inches,
        "color": material.color,
        "quantity_on_hand": material.quantity_on_hand,
        "quantity_reserved": material.quantity_reserved,
        "quantity_available": material.quantity_available,
        "unit": material.unit,
        "reorder_point": material.reorder_point,
        "reorder_quantity": material.reorder_quantity,
        "is_low_stock": material.is_low_stock,
        "cost_per_unit": material.cost_per_unit,
        "supplier_id": material.supplier_id,
        "created_at": material.created_at.isoformat() if material.created_at else None,
        "updated_at": material.updated_at.isoformat() if material.updated_at else None
    }


@app.post("/coo/materials/{material_id}/transaction", tags=["Raw Materials"])
async def record_material_transaction(
    material_id: int,
    transaction: MaterialTransactionCreate,
    db: Session = Depends(get_db_session)
):
    """Record a material transaction (receive, use, adjust)."""
    material = db.query(RawMaterial).filter(RawMaterial.id == material_id).first()
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")

    # Calculate new quantity
    if transaction.transaction_type == "receive":
        new_quantity = material.quantity_on_hand + abs(transaction.quantity)
    elif transaction.transaction_type == "use":
        new_quantity = material.quantity_on_hand - abs(transaction.quantity)
        if new_quantity < 0:
            raise HTTPException(status_code=400, detail="Insufficient inventory")
    elif transaction.transaction_type == "adjust":
        new_quantity = transaction.quantity  # Direct set
    else:
        raise HTTPException(status_code=400, detail=f"Invalid transaction type: {transaction.transaction_type}")

    old_quantity = material.quantity_on_hand

    # Update material
    material.quantity_on_hand = new_quantity
    material.quantity_available = new_quantity - material.quantity_reserved
    material.is_low_stock = new_quantity <= material.reorder_point

    # Update cost if provided
    if transaction.unit_cost and transaction.transaction_type == "receive":
        material.cost_per_unit = transaction.unit_cost
        material.last_cost_update = datetime.utcnow()

    # Record transaction
    db_txn = MaterialTransaction(
        material_id=material_id,
        transaction_type=transaction.transaction_type,
        quantity=transaction.quantity if transaction.transaction_type != "use" else -abs(transaction.quantity),
        unit=material.unit,
        unit_cost=transaction.unit_cost,
        total_cost=(transaction.unit_cost or 0) * abs(transaction.quantity),
        reference_type=transaction.reference_type,
        reference_id=transaction.reference_id,
        notes=transaction.notes
    )
    db.add(db_txn)

    # Check for low stock alert
    was_low = old_quantity <= material.reorder_point
    is_low = new_quantity <= material.reorder_point

    if is_low and not was_low:
        # Just went low - publish alert
        publish_inventory_low(
            item_type="raw_material",
            item_id=material.id,
            item_name=material.name,
            current_quantity=new_quantity,
            reorder_point=material.reorder_point,
            sku=material.sku
        )

        # Create internal alert
        alert = COOAlert(
            severity=AlertSeverity.WARNING,
            category="inventory",
            title=f"Low Stock: {material.name}",
            message=f"{material.name} ({material.sku}) has {new_quantity} {material.unit} remaining",
            related_type="raw_material",
            related_id=material.id,
            suggested_action=f"Reorder {material.reorder_quantity} {material.unit}"
        )
        db.add(alert)

    elif not is_low and was_low and transaction.transaction_type == "receive":
        # Was low, now restocked - publish notification
        publish_inventory_restocked(
            item_type="raw_material",
            item_id=material.id,
            item_name=material.name,
            new_quantity=new_quantity,
            sku=material.sku
        )

    db.commit()

    return {
        "success": True,
        "material_id": material_id,
        "old_quantity": old_quantity,
        "new_quantity": new_quantity,
        "is_low_stock": material.is_low_stock
    }


# ============================================================================
# PRODUCTION ENDPOINTS
# ============================================================================

@app.get("/coo/production", tags=["Production"])
async def list_production_runs(
    status: Optional[str] = None,
    stage: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    db: Session = Depends(get_db_session)
):
    """List production runs."""
    query = db.query(ProductionRun)

    if status:
        query = query.filter(ProductionRun.status == status)
    if stage:
        query = query.filter(ProductionRun.current_stage == ProductionStage(stage))

    runs = query.order_by(ProductionRun.priority, ProductionRun.created_at.desc()).limit(limit).all()

    return [
        {
            "id": r.id,
            "run_number": r.run_number,
            "product_name": r.product_name,
            "product_sku": r.product_sku,
            "quantity_planned": r.quantity_planned,
            "quantity_completed": r.quantity_completed,
            "current_stage": r.current_stage.value if r.current_stage else None,
            "status": r.status,
            "priority": r.priority,
            "planned_start_date": r.planned_start_date.isoformat() if r.planned_start_date else None,
            "planned_end_date": r.planned_end_date.isoformat() if r.planned_end_date else None
        }
        for r in runs
    ]


@app.post("/coo/production", tags=["Production"])
async def create_production_run(
    run: ProductionRunCreate,
    db: Session = Depends(get_db_session)
):
    """Create a new production run."""
    # Generate run number
    year = datetime.utcnow().year
    count = db.query(ProductionRun).filter(
        ProductionRun.run_number.like(f"PR-{year}-%")
    ).count()
    run_number = f"PR-{year}-{count + 1:03d}"

    db_run = ProductionRun(
        run_number=run_number,
        product_name=run.product_name,
        product_sku=run.product_sku,
        shopify_product_id=run.shopify_product_id,
        quantity_planned=run.quantity_planned,
        planned_start_date=run.planned_start_date,
        planned_end_date=run.planned_end_date,
        priority=run.priority,
        notes=run.notes,
        status="planned",
        current_stage=ProductionStage.CUTTING
    )

    db.add(db_run)
    db.commit()
    db.refresh(db_run)

    return {
        "id": db_run.id,
        "run_number": db_run.run_number,
        "product_name": db_run.product_name,
        "status": db_run.status
    }


@app.post("/coo/production/{run_id}/start", tags=["Production"])
async def start_production_run(run_id: int, db: Session = Depends(get_db_session)):
    """Start a production run."""
    run = db.query(ProductionRun).filter(ProductionRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Production run not found")

    if run.status != "planned":
        raise HTTPException(status_code=400, detail=f"Cannot start run with status: {run.status}")

    run.status = "in_progress"
    run.actual_start_date = datetime.utcnow()

    # Log first stage
    stage_log = ProductionStageLog(
        production_run_id=run.id,
        stage=ProductionStage.CUTTING,
        quantity=run.quantity_planned,
        started_at=datetime.utcnow()
    )
    db.add(stage_log)
    db.commit()

    return {"success": True, "run_number": run.run_number, "status": run.status}


@app.post("/coo/production/{run_id}/stage", tags=["Production"])
async def update_production_stage(
    run_id: int,
    update: ProductionStageUpdate,
    db: Session = Depends(get_db_session)
):
    """Update production stage (move items through production)."""
    run = db.query(ProductionRun).filter(ProductionRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Production run not found")

    new_stage = ProductionStage(update.stage)

    # Complete current stage log
    current_log = db.query(ProductionStageLog).filter(
        ProductionStageLog.production_run_id == run.id,
        ProductionStageLog.stage == run.current_stage,
        ProductionStageLog.completed_at == None
    ).first()

    if current_log:
        current_log.completed_at = datetime.utcnow()

    # Create new stage log
    new_log = ProductionStageLog(
        production_run_id=run.id,
        stage=new_stage,
        quantity=update.quantity,
        started_at=datetime.utcnow(),
        worker=update.worker,
        notes=update.notes
    )
    db.add(new_log)

    # Update run
    run.current_stage = new_stage

    if new_stage == ProductionStage.COMPLETE:
        run.quantity_completed = update.quantity
        run.status = "completed"
        run.actual_end_date = datetime.utcnow()

        # Publish completion event
        publish_production_complete(
            run_id=run.id,
            run_number=run.run_number,
            product_name=run.product_name,
            quantity_completed=update.quantity
        )

    db.commit()

    return {
        "success": True,
        "run_number": run.run_number,
        "current_stage": run.current_stage.value,
        "status": run.status
    }


# ============================================================================
# FINISHED GOODS ENDPOINTS
# ============================================================================

@app.get("/coo/finished-goods", tags=["Finished Goods"])
async def list_finished_goods(
    low_stock_only: bool = False,
    limit: int = Query(default=100, le=500),
    db: Session = Depends(get_db_session)
):
    """List finished goods inventory."""
    query = db.query(FinishedGoodsInventory)

    if low_stock_only:
        query = query.filter(FinishedGoodsInventory.is_low_stock == True)

    items = query.order_by(FinishedGoodsInventory.product_title).limit(limit).all()

    return [
        {
            "id": item.id,
            "shopify_product_id": item.shopify_product_id,
            "shopify_variant_id": item.shopify_variant_id,
            "product_title": item.product_title,
            "variant_title": item.variant_title,
            "sku": item.sku,
            "quantity_on_hand": item.quantity_on_hand,
            "quantity_available": item.quantity_available,
            "reorder_point": item.reorder_point,
            "is_low_stock": item.is_low_stock,
            "last_synced_at": item.last_synced_at.isoformat() if item.last_synced_at else None
        }
        for item in items
    ]


@app.post("/coo/finished-goods/sync", tags=["Finished Goods"])
async def sync_finished_goods_from_shopify(
    clear_old: bool = Query(default=False, description="Clear non-tagged products from database"),
    db: Session = Depends(get_db_session)
):
    """
    Sync sub-assembly inventory from Shopify.

    Only syncs products with 'trackOutOfStockReport' tag - these are the real
    sub-assembly inventory items, not the virtual finished goods.
    """
    access_token = get_shopify_token(db)
    if not access_token:
        raise HTTPException(
            status_code=503,
            detail="Shopify not connected. Visit /coo/auth/shopify to authenticate."
        )

    try:
        import httpx

        # GraphQL query to fetch ONLY products with trackOutOfStockReport tag
        # Using pagination to handle large catalogs
        query_template = """
        query ($cursor: String) {
            products(first: 250, after: $cursor, query: "tag:trackOutOfStockReport") {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                edges {
                    node {
                        id
                        title
                        tags
                        variants(first: 100) {
                            edges {
                                node {
                                    id
                                    title
                                    sku
                                    inventoryQuantity
                                    inventoryItem {
                                        id
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """

        synced_count = 0
        synced_variant_ids = set()
        cursor = None
        has_next_page = True

        async with httpx.AsyncClient(timeout=60.0) as client:
            while has_next_page:
                response = await client.post(
                    f"https://{settings.shopify_store}/admin/api/2026-01/graphql.json",
                    headers={
                        "X-Shopify-Access-Token": access_token,
                        "Content-Type": "application/json"
                    },
                    json={
                        "query": query_template,
                        "variables": {"cursor": cursor}
                    }
                )

                if response.status_code != 200:
                    raise HTTPException(status_code=500, detail=f"Shopify API error: {response.status_code}")

                data = response.json()

                # Check for GraphQL errors
                if "errors" in data:
                    raise HTTPException(status_code=500, detail=f"GraphQL error: {data['errors']}")

                products_data = data.get("data", {}).get("products", {})
                products = products_data.get("edges", [])
                page_info = products_data.get("pageInfo", {})

                for product_edge in products:
                    product = product_edge["node"]
                    product_id = product["id"].split("/")[-1]
                    product_title = product["title"]

                    for variant_edge in product.get("variants", {}).get("edges", []):
                        variant = variant_edge["node"]
                        variant_id = variant["id"].split("/")[-1]
                        synced_variant_ids.add(variant_id)

                        # Upsert inventory record
                        existing = db.query(FinishedGoodsInventory).filter(
                            FinishedGoodsInventory.shopify_variant_id == variant_id
                        ).first()

                        quantity = variant.get("inventoryQuantity", 0)
                        reorder_point = existing.reorder_point if existing else settings.default_reorder_point

                        if existing:
                            old_quantity = existing.quantity_on_hand
                            existing.product_title = product_title
                            existing.variant_title = variant.get("title")
                            existing.sku = variant.get("sku")
                            existing.quantity_on_hand = quantity
                            existing.quantity_available = quantity
                            existing.is_low_stock = quantity <= reorder_point
                            existing.last_synced_at = datetime.utcnow()

                            # Check for low stock transition
                            if quantity <= reorder_point and old_quantity > reorder_point:
                                publish_inventory_low(
                                    item_type="sub_assembly",
                                    item_id=existing.id,
                                    item_name=f"{product_title} - {variant.get('title', '')}",
                                    current_quantity=quantity,
                                    reorder_point=reorder_point,
                                    sku=variant.get("sku")
                                )
                        else:
                            new_item = FinishedGoodsInventory(
                                shopify_product_id=product_id,
                                shopify_variant_id=variant_id,
                                shopify_inventory_item_id=variant.get("inventoryItem", {}).get("id", "").split("/")[-1] if variant.get("inventoryItem") else None,
                                product_title=product_title,
                                variant_title=variant.get("title"),
                                sku=variant.get("sku"),
                                quantity_on_hand=quantity,
                                quantity_available=quantity,
                                reorder_point=reorder_point,
                                is_low_stock=quantity <= reorder_point,
                                last_synced_at=datetime.utcnow()
                            )
                            db.add(new_item)

                        synced_count += 1

                # Check for more pages
                has_next_page = page_info.get("hasNextPage", False)
                cursor = page_info.get("endCursor")

        # Optionally clear products that no longer have the tag
        cleared_count = 0
        if clear_old:
            # Delete records not in this sync (they don't have the tag anymore)
            old_records = db.query(FinishedGoodsInventory).filter(
                ~FinishedGoodsInventory.shopify_variant_id.in_(synced_variant_ids)
            ).all()
            cleared_count = len(old_records)
            for record in old_records:
                db.delete(record)

        db.commit()

        return {
            "success": True,
            "synced_count": synced_count,
            "cleared_count": cleared_count,
            "filter": "tag:trackOutOfStockReport",
            "synced_at": datetime.utcnow().isoformat()
        }

    except httpx.HTTPError as e:
        raise HTTPException(status_code=500, detail=f"Shopify request failed: {str(e)}")


# ============================================================================
# SUPPLIERS ENDPOINTS
# ============================================================================

@app.get("/coo/suppliers", tags=["Suppliers"])
async def list_suppliers(db: Session = Depends(get_db_session)):
    """List all suppliers."""
    suppliers = db.query(Supplier).filter(Supplier.is_active == True).all()
    return [
        {
            "id": s.id,
            "name": s.name,
            "contact_name": s.contact_name,
            "email": s.email,
            "phone": s.phone,
            "avg_lead_time_days": s.avg_lead_time_days,
            "payment_terms": s.payment_terms
        }
        for s in suppliers
    ]


@app.post("/coo/suppliers", tags=["Suppliers"])
async def create_supplier(supplier: SupplierCreate, db: Session = Depends(get_db_session)):
    """Create a new supplier."""
    db_supplier = Supplier(
        name=supplier.name,
        contact_name=supplier.contact_name,
        email=supplier.email,
        phone=supplier.phone,
        address=supplier.address,
        avg_lead_time_days=supplier.avg_lead_time_days,
        payment_terms=supplier.payment_terms
    )
    db.add(db_supplier)
    db.commit()
    db.refresh(db_supplier)

    return {"id": db_supplier.id, "name": db_supplier.name}


# ============================================================================
# ALERTS ENDPOINTS
# ============================================================================

@app.get("/coo/alerts", tags=["Alerts"])
async def list_alerts(
    category: Optional[str] = None,
    unresolved_only: bool = True,
    limit: int = Query(default=50, le=200),
    db: Session = Depends(get_db_session)
):
    """List COO alerts."""
    query = db.query(COOAlert)

    if category:
        query = query.filter(COOAlert.category == category)
    if unresolved_only:
        query = query.filter(COOAlert.is_resolved == False)

    alerts = query.order_by(COOAlert.created_at.desc()).limit(limit).all()

    return [
        {
            "id": a.id,
            "severity": a.severity.value if a.severity else None,
            "category": a.category,
            "title": a.title,
            "message": a.message,
            "suggested_action": a.suggested_action,
            "is_read": a.is_read,
            "is_resolved": a.is_resolved,
            "created_at": a.created_at.isoformat() if a.created_at else None
        }
        for a in alerts
    ]


@app.put("/coo/alerts/{alert_id}/resolve", tags=["Alerts"])
async def resolve_alert(alert_id: int, db: Session = Depends(get_db_session)):
    """Mark an alert as resolved."""
    alert = db.query(COOAlert).filter(COOAlert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")

    alert.is_resolved = True
    alert.resolved_at = datetime.utcnow()
    db.commit()

    return {"success": True, "alert_id": alert_id}


# ============================================================================
# PURCHASE ORDER ENDPOINTS
# ============================================================================

PO_APPROVAL_THRESHOLD = 500.0  # POs over this amount require CEO approval


@app.get("/coo/purchase-orders", tags=["Purchase Orders"])
async def list_purchase_orders(
    status: Optional[str] = None,
    supplier_id: Optional[int] = None,
    limit: int = Query(default=50, le=200),
    db: Session = Depends(get_db_session)
):
    """List purchase orders."""
    query = db.query(PurchaseOrder)

    if status:
        query = query.filter(PurchaseOrder.status == POStatus(status))
    if supplier_id:
        query = query.filter(PurchaseOrder.supplier_id == supplier_id)

    pos = query.order_by(PurchaseOrder.created_at.desc()).limit(limit).all()

    return [
        {
            "id": po.id,
            "po_number": po.po_number,
            "supplier_id": po.supplier_id,
            "supplier_name": po.supplier.name if po.supplier else None,
            "status": po.status.value if po.status else None,
            "subtotal": po.subtotal,
            "shipping_cost": po.shipping_cost,
            "total": po.total,
            "item_count": len(po.items) if po.items else 0,
            "source": po.source,
            "expected_delivery": po.expected_delivery.isoformat() if po.expected_delivery else None,
            "created_at": po.created_at.isoformat() if po.created_at else None,
        }
        for po in pos
    ]


@app.post("/coo/purchase-orders", tags=["Purchase Orders"])
async def create_purchase_order(
    data: PurchaseOrderCreate,
    db: Session = Depends(get_db_session)
):
    """Create a new purchase order."""
    # Validate supplier
    supplier = db.query(Supplier).filter(Supplier.id == data.supplier_id).first()
    if not supplier:
        raise HTTPException(status_code=404, detail="Supplier not found")

    # Generate PO number
    year = datetime.utcnow().year
    count = db.query(PurchaseOrder).filter(
        PurchaseOrder.po_number.like(f"PO-{year}-%")
    ).count()
    po_number = f"PO-{year}-{count + 1:03d}"

    # Calculate totals
    subtotal = sum(item.quantity * item.unit_cost for item in data.items)
    total = subtotal + data.shipping_cost

    requires_approval = total > PO_APPROVAL_THRESHOLD

    po = PurchaseOrder(
        po_number=po_number,
        supplier_id=data.supplier_id,
        status=POStatus.PENDING_APPROVAL if requires_approval else POStatus.DRAFT,
        subtotal=subtotal,
        shipping_cost=data.shipping_cost,
        total=total,
        expected_delivery=data.expected_delivery or (
            datetime.utcnow() + timedelta(days=supplier.avg_lead_time_days or 14)
        ),
        requires_approval=requires_approval,
        source=data.source,
        source_reference=data.source_reference,
        notes=data.notes,
    )
    db.add(po)
    db.flush()

    # Add line items
    for item_data in data.items:
        material = db.query(RawMaterial).filter(RawMaterial.id == item_data.material_id).first()
        if not material:
            raise HTTPException(status_code=404, detail=f"Material {item_data.material_id} not found")

        po_item = PurchaseOrderItem(
            purchase_order_id=po.id,
            material_id=item_data.material_id,
            quantity=item_data.quantity,
            unit=material.unit,
            unit_cost=item_data.unit_cost,
            total_cost=item_data.quantity * item_data.unit_cost,
            notes=item_data.notes,
        )
        db.add(po_item)

    db.commit()
    db.refresh(po)

    # If approval needed, send to CEO
    if requires_approval:
        publish_po_approval_needed(
            po_id=po.id,
            po_number=po_number,
            supplier_name=supplier.name,
            total=total,
        )

    return {
        "id": po.id,
        "po_number": po_number,
        "status": po.status.value,
        "total": total,
        "requires_approval": requires_approval,
    }


@app.get("/coo/purchase-orders/{po_id}", tags=["Purchase Orders"])
async def get_purchase_order(po_id: int, db: Session = Depends(get_db_session)):
    """Get purchase order details with line items."""
    po = db.query(PurchaseOrder).filter(PurchaseOrder.id == po_id).first()
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")

    return {
        "id": po.id,
        "po_number": po.po_number,
        "supplier_id": po.supplier_id,
        "supplier_name": po.supplier.name if po.supplier else None,
        "status": po.status.value if po.status else None,
        "subtotal": po.subtotal,
        "shipping_cost": po.shipping_cost,
        "total": po.total,
        "order_date": po.order_date.isoformat() if po.order_date else None,
        "expected_delivery": po.expected_delivery.isoformat() if po.expected_delivery else None,
        "actual_delivery": po.actual_delivery.isoformat() if po.actual_delivery else None,
        "requires_approval": po.requires_approval,
        "approved_by": po.approved_by,
        "approved_at": po.approved_at.isoformat() if po.approved_at else None,
        "source": po.source,
        "notes": po.notes,
        "items": [
            {
                "id": item.id,
                "material_id": item.material_id,
                "material_name": item.material.name if item.material else None,
                "quantity": item.quantity,
                "unit": item.unit,
                "unit_cost": item.unit_cost,
                "total_cost": item.total_cost,
                "quantity_received": item.quantity_received,
            }
            for item in po.items
        ],
        "created_at": po.created_at.isoformat() if po.created_at else None,
    }


@app.post("/coo/purchase-orders/{po_id}/approve", tags=["Purchase Orders"])
async def approve_purchase_order(
    po_id: int,
    approved_by: str = "ceo",
    notes: Optional[str] = None,
    db: Session = Depends(get_db_session)
):
    """Approve a purchase order."""
    po = db.query(PurchaseOrder).filter(PurchaseOrder.id == po_id).first()
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")

    if po.status != POStatus.PENDING_APPROVAL:
        raise HTTPException(status_code=400, detail=f"Cannot approve PO with status: {po.status.value}")

    po.status = POStatus.APPROVED
    po.approved_by = approved_by
    po.approved_at = datetime.utcnow()
    po.approval_notes = notes
    db.commit()

    return {"success": True, "po_number": po.po_number, "status": "approved"}


@app.post("/coo/purchase-orders/{po_id}/send", tags=["Purchase Orders"])
async def send_purchase_order(po_id: int, db: Session = Depends(get_db_session)):
    """Mark a purchase order as sent to supplier."""
    po = db.query(PurchaseOrder).filter(PurchaseOrder.id == po_id).first()
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")

    if po.status not in (POStatus.DRAFT, POStatus.APPROVED):
        raise HTTPException(status_code=400, detail=f"Cannot send PO with status: {po.status.value}")

    po.status = POStatus.SENT
    po.order_date = datetime.utcnow()
    db.commit()

    return {"success": True, "po_number": po.po_number, "status": "sent"}


@app.post("/coo/purchase-orders/{po_id}/receive", tags=["Purchase Orders"])
async def receive_purchase_order(
    po_id: int,
    db: Session = Depends(get_db_session)
):
    """
    Receive a purchase order - updates material inventory and records transactions.
    Receives all items in full. For partial receiving, use the item-level endpoint.
    """
    po = db.query(PurchaseOrder).filter(PurchaseOrder.id == po_id).first()
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")

    if po.status in (POStatus.RECEIVED, POStatus.CANCELLED):
        raise HTTPException(status_code=400, detail=f"Cannot receive PO with status: {po.status.value}")

    received_items = []
    for item in po.items:
        remaining = item.quantity - item.quantity_received
        if remaining <= 0:
            continue

        # Update material inventory
        material = db.query(RawMaterial).filter(RawMaterial.id == item.material_id).first()
        if material:
            old_qty = material.quantity_on_hand
            material.quantity_on_hand += remaining
            material.quantity_available = material.quantity_on_hand - material.quantity_reserved
            material.is_low_stock = material.quantity_on_hand <= material.reorder_point
            material.cost_per_unit = item.unit_cost
            material.last_cost_update = datetime.utcnow()

            # Record transaction
            txn = MaterialTransaction(
                material_id=item.material_id,
                transaction_type="receive",
                quantity=remaining,
                unit=item.unit,
                unit_cost=item.unit_cost,
                total_cost=remaining * item.unit_cost,
                reference_type="purchase_order",
                reference_id=str(po.id),
                notes=f"Received from PO {po.po_number}",
            )
            db.add(txn)

            # Check restock event
            if old_qty <= material.reorder_point and material.quantity_on_hand > material.reorder_point:
                publish_inventory_restocked(
                    item_type="raw_material",
                    item_id=material.id,
                    item_name=material.name,
                    new_quantity=material.quantity_on_hand,
                    sku=material.sku,
                )

        item.quantity_received = item.quantity
        item.received_at = datetime.utcnow()
        received_items.append(item.material_id)

    po.status = POStatus.RECEIVED
    po.actual_delivery = datetime.utcnow()
    db.commit()

    return {
        "success": True,
        "po_number": po.po_number,
        "status": "received",
        "items_received": len(received_items),
    }


@app.post("/coo/purchase-orders/{po_id}/cancel", tags=["Purchase Orders"])
async def cancel_purchase_order(po_id: int, db: Session = Depends(get_db_session)):
    """Cancel a purchase order."""
    po = db.query(PurchaseOrder).filter(PurchaseOrder.id == po_id).first()
    if not po:
        raise HTTPException(status_code=404, detail="Purchase order not found")

    if po.status in (POStatus.RECEIVED, POStatus.CANCELLED):
        raise HTTPException(status_code=400, detail=f"Cannot cancel PO with status: {po.status.value}")

    po.status = POStatus.CANCELLED
    db.commit()

    return {"success": True, "po_number": po.po_number, "status": "cancelled"}


# ============================================================================
# REORDER AUTOMATION ENDPOINTS
# ============================================================================

@app.get("/coo/reorder-suggestions", tags=["Reorder"])
async def get_reorder_suggestions(db: Session = Depends(get_db_session)):
    """
    Get materials that need reordering based on reorder points.
    Calculates dynamic reorder points from recent usage velocity.
    """
    low_materials = db.query(RawMaterial).filter(
        RawMaterial.is_active == True,
        RawMaterial.is_low_stock == True,
    ).all()

    suggestions = []
    for m in low_materials:
        # Calculate usage velocity from last 30 days of 'use' transactions
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        usage_txns = db.query(MaterialTransaction).filter(
            MaterialTransaction.material_id == m.id,
            MaterialTransaction.transaction_type == "use",
            MaterialTransaction.created_at >= thirty_days_ago,
        ).all()

        total_used = sum(abs(t.quantity) for t in usage_txns)
        daily_velocity = total_used / 30 if total_used > 0 else 0

        # Days of stock remaining
        days_remaining = m.quantity_on_hand / daily_velocity if daily_velocity > 0 else float('inf')

        # Supplier lead time
        supplier = db.query(Supplier).filter(Supplier.id == m.supplier_id).first() if m.supplier_id else None
        lead_time = supplier.avg_lead_time_days if supplier else 14

        # Urgency
        if days_remaining <= lead_time:
            urgency = "critical"
        elif days_remaining <= lead_time * 1.5:
            urgency = "high"
        else:
            urgency = "normal"

        suggestions.append({
            "material_id": m.id,
            "sku": m.sku,
            "name": m.name,
            "quantity_on_hand": m.quantity_on_hand,
            "reorder_point": m.reorder_point,
            "reorder_quantity": m.reorder_quantity,
            "daily_velocity": round(daily_velocity, 2),
            "days_remaining": round(days_remaining, 1) if days_remaining != float('inf') else None,
            "supplier_id": m.supplier_id,
            "supplier_name": supplier.name if supplier else None,
            "lead_time_days": lead_time,
            "urgency": urgency,
            "estimated_cost": round(m.reorder_quantity * m.cost_per_unit, 2),
        })

    # Sort by urgency
    urgency_order = {"critical": 0, "high": 1, "normal": 2}
    suggestions.sort(key=lambda x: urgency_order.get(x["urgency"], 3))

    return {"suggestions": suggestions, "total": len(suggestions)}


@app.post("/coo/reorder-suggestions/generate-pos", tags=["Reorder"])
async def auto_generate_purchase_orders(
    urgency_filter: Optional[str] = Query(None, description="Filter: critical, high, normal"),
    db: Session = Depends(get_db_session)
):
    """
    Auto-generate draft purchase orders from reorder suggestions.
    Groups items by supplier into single POs.
    """
    low_materials = db.query(RawMaterial).filter(
        RawMaterial.is_active == True,
        RawMaterial.is_low_stock == True,
        RawMaterial.supplier_id != None,
    ).all()

    if not low_materials:
        return {"success": True, "message": "No materials need reordering", "pos_created": 0}

    # Group by supplier
    by_supplier = {}
    for m in low_materials:
        if m.supplier_id not in by_supplier:
            by_supplier[m.supplier_id] = []
        by_supplier[m.supplier_id].append(m)

    created_pos = []
    year = datetime.utcnow().year

    for supplier_id, materials in by_supplier.items():
        supplier = db.query(Supplier).filter(Supplier.id == supplier_id).first()
        if not supplier:
            continue

        count = db.query(PurchaseOrder).filter(
            PurchaseOrder.po_number.like(f"PO-{year}-%")
        ).count()
        po_number = f"PO-{year}-{count + 1:03d}"

        subtotal = sum(m.reorder_quantity * m.cost_per_unit for m in materials)
        total = subtotal
        requires_approval = total > PO_APPROVAL_THRESHOLD

        po = PurchaseOrder(
            po_number=po_number,
            supplier_id=supplier_id,
            status=POStatus.PENDING_APPROVAL if requires_approval else POStatus.DRAFT,
            subtotal=subtotal,
            total=total,
            expected_delivery=datetime.utcnow() + timedelta(days=supplier.avg_lead_time_days or 14),
            requires_approval=requires_approval,
            source="auto_reorder",
            notes=f"Auto-generated from reorder suggestions for {len(materials)} materials",
        )
        db.add(po)
        db.flush()

        for m in materials:
            po_item = PurchaseOrderItem(
                purchase_order_id=po.id,
                material_id=m.id,
                quantity=m.reorder_quantity,
                unit=m.unit,
                unit_cost=m.cost_per_unit,
                total_cost=m.reorder_quantity * m.cost_per_unit,
            )
            db.add(po_item)

        if requires_approval:
            publish_po_approval_needed(
                po_id=po.id,
                po_number=po_number,
                supplier_name=supplier.name,
                total=total,
            )

        created_pos.append({
            "po_number": po_number,
            "supplier": supplier.name,
            "items": len(materials),
            "total": total,
            "requires_approval": requires_approval,
        })

    db.commit()

    return {
        "success": True,
        "pos_created": len(created_pos),
        "purchase_orders": created_pos,
    }


@app.post("/coo/materials/{material_id}/set-reorder", tags=["Reorder"])
async def set_reorder_params(
    material_id: int,
    reorder_point: float,
    reorder_quantity: float,
    db: Session = Depends(get_db_session)
):
    """Set reorder point and quantity for a material."""
    material = db.query(RawMaterial).filter(RawMaterial.id == material_id).first()
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")

    material.reorder_point = reorder_point
    material.reorder_quantity = reorder_quantity
    material.is_low_stock = material.quantity_on_hand <= reorder_point
    db.commit()

    return {
        "success": True,
        "material_id": material_id,
        "reorder_point": reorder_point,
        "reorder_quantity": reorder_quantity,
        "is_low_stock": material.is_low_stock,
    }


# ============================================================================
# PRODUCTION COST TRACKING ENDPOINTS
# ============================================================================

@app.get("/coo/production/{run_id}/costs", tags=["Production Costs"])
async def get_production_costs(run_id: int, db: Session = Depends(get_db_session)):
    """
    Get cost breakdown for a production run.
    Calculates material costs from transactions linked to this run.
    """
    run = db.query(ProductionRun).filter(ProductionRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Production run not found")

    # Get all material transactions for this run
    txns = db.query(MaterialTransaction).filter(
        MaterialTransaction.reference_type == "production_run",
        MaterialTransaction.reference_id == str(run_id),
    ).all()

    material_costs = []
    total_material_cost = 0
    for txn in txns:
        material = db.query(RawMaterial).filter(RawMaterial.id == txn.material_id).first()
        cost = abs(txn.total_cost or 0)
        total_material_cost += cost
        material_costs.append({
            "material_id": txn.material_id,
            "material_name": material.name if material else "Unknown",
            "sku": material.sku if material else None,
            "quantity_used": abs(txn.quantity),
            "unit": txn.unit,
            "unit_cost": txn.unit_cost,
            "total_cost": cost,
        })

    # Planned cost from materials_allocated
    planned_cost = 0
    if run.materials_allocated:
        for alloc in run.materials_allocated:
            mat = db.query(RawMaterial).filter(RawMaterial.id == alloc.get("material_id")).first()
            if mat:
                planned_cost += alloc.get("quantity", 0) * (mat.cost_per_unit or 0)

    qty = run.quantity_completed or run.quantity_planned
    cost_per_unit = total_material_cost / qty if qty > 0 else 0

    return {
        "run_id": run.id,
        "run_number": run.run_number,
        "product_name": run.product_name,
        "status": run.status,
        "quantity_planned": run.quantity_planned,
        "quantity_completed": run.quantity_completed,
        "material_costs": material_costs,
        "total_material_cost": round(total_material_cost, 2),
        "planned_material_cost": round(planned_cost, 2),
        "cost_variance": round(total_material_cost - planned_cost, 2) if planned_cost else None,
        "cost_per_unit": round(cost_per_unit, 2),
    }


@app.post("/coo/production/{run_id}/publish-costs", tags=["Production Costs"])
async def publish_run_costs(run_id: int, db: Session = Depends(get_db_session)):
    """Publish actual production costs to CFO module."""
    run = db.query(ProductionRun).filter(ProductionRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Production run not found")

    txns = db.query(MaterialTransaction).filter(
        MaterialTransaction.reference_type == "production_run",
        MaterialTransaction.reference_id == str(run_id),
    ).all()

    total_material_cost = sum(abs(t.total_cost or 0) for t in txns)
    qty = run.quantity_completed or run.quantity_planned
    cost_per_unit = total_material_cost / qty if qty > 0 else 0

    publish_production_costs(
        run_id=run.id,
        run_number=run.run_number,
        product_name=run.product_name,
        total_material_cost=total_material_cost,
        cost_per_unit=cost_per_unit,
        quantity_completed=qty,
    )

    return {
        "success": True,
        "run_number": run.run_number,
        "total_material_cost": round(total_material_cost, 2),
        "cost_per_unit": round(cost_per_unit, 2),
        "published_to": "cfo",
    }


# ============================================================================
# MATERIAL RESERVATION ENDPOINTS
# ============================================================================

@app.post("/coo/materials/reserve", tags=["Material Reservation"])
async def reserve_materials(
    reservations: List[MaterialReservation],
    db: Session = Depends(get_db_session)
):
    """
    Reserve materials for a production run.
    Checks availability and prevents over-allocation.
    """
    reserved_items = []

    for res in reservations:
        material = db.query(RawMaterial).filter(RawMaterial.id == res.material_id).first()
        if not material:
            raise HTTPException(status_code=404, detail=f"Material {res.material_id} not found")

        if material.quantity_available < res.quantity:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient {material.name}: {material.quantity_available} available, {res.quantity} requested"
            )

        material.quantity_reserved += res.quantity
        material.quantity_available = material.quantity_on_hand - material.quantity_reserved

        # Record transaction
        txn = MaterialTransaction(
            material_id=res.material_id,
            transaction_type="reserve",
            quantity=-res.quantity,
            unit=material.unit,
            unit_cost=material.cost_per_unit,
            total_cost=res.quantity * (material.cost_per_unit or 0),
            reference_type="production_run",
            reference_id=str(res.production_run_id),
            notes=f"Reserved for production run {res.production_run_id}",
        )
        db.add(txn)

        reserved_items.append({
            "material_id": material.id,
            "name": material.name,
            "quantity_reserved": res.quantity,
            "remaining_available": material.quantity_available,
        })

    # Update production run materials_allocated
    if reservations:
        run = db.query(ProductionRun).filter(
            ProductionRun.id == reservations[0].production_run_id
        ).first()
        if run:
            existing = run.materials_allocated or []
            for res in reservations:
                existing.append({"material_id": res.material_id, "quantity": res.quantity})
            run.materials_allocated = existing

    db.commit()

    return {"success": True, "reserved": reserved_items}


@app.post("/coo/materials/release", tags=["Material Reservation"])
async def release_materials(
    material_id: int,
    quantity: float,
    production_run_id: int,
    db: Session = Depends(get_db_session)
):
    """Release reserved materials back to available inventory."""
    material = db.query(RawMaterial).filter(RawMaterial.id == material_id).first()
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")

    release_qty = min(quantity, material.quantity_reserved)
    material.quantity_reserved -= release_qty
    material.quantity_available = material.quantity_on_hand - material.quantity_reserved

    txn = MaterialTransaction(
        material_id=material_id,
        transaction_type="release",
        quantity=release_qty,
        unit=material.unit,
        reference_type="production_run",
        reference_id=str(production_run_id),
        notes=f"Released from production run {production_run_id}",
    )
    db.add(txn)
    db.commit()

    return {
        "success": True,
        "material_id": material_id,
        "quantity_released": release_qty,
        "quantity_reserved": material.quantity_reserved,
        "quantity_available": material.quantity_available,
    }


@app.post("/coo/production/{run_id}/use-materials", tags=["Material Reservation"])
async def use_reserved_materials(
    run_id: int,
    db: Session = Depends(get_db_session)
):
    """
    Convert reserved materials to used for a production run.
    Decreases on_hand and reserved, records 'use' transactions for cost tracking.
    """
    run = db.query(ProductionRun).filter(ProductionRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Production run not found")

    if not run.materials_allocated:
        raise HTTPException(status_code=400, detail="No materials allocated to this run")

    used_items = []
    for alloc in run.materials_allocated:
        mat_id = alloc.get("material_id")
        qty = alloc.get("quantity", 0)
        if not mat_id or qty <= 0:
            continue

        material = db.query(RawMaterial).filter(RawMaterial.id == mat_id).first()
        if not material:
            continue

        # Move from reserved to used
        actual_reserve = min(qty, material.quantity_reserved)
        material.quantity_reserved -= actual_reserve
        material.quantity_on_hand -= qty
        material.quantity_available = material.quantity_on_hand - material.quantity_reserved
        material.is_low_stock = material.quantity_on_hand <= material.reorder_point

        txn = MaterialTransaction(
            material_id=mat_id,
            transaction_type="use",
            quantity=-qty,
            unit=material.unit,
            unit_cost=material.cost_per_unit,
            total_cost=qty * (material.cost_per_unit or 0),
            reference_type="production_run",
            reference_id=str(run_id),
            notes=f"Used in production run {run.run_number}",
        )
        db.add(txn)

        # Check low stock
        if material.is_low_stock:
            publish_inventory_low(
                item_type="raw_material",
                item_id=material.id,
                item_name=material.name,
                current_quantity=material.quantity_on_hand,
                reorder_point=material.reorder_point,
                sku=material.sku,
            )

        used_items.append({
            "material_id": mat_id,
            "name": material.name,
            "quantity_used": qty,
            "cost": round(qty * (material.cost_per_unit or 0), 2),
        })

    db.commit()

    return {
        "success": True,
        "run_number": run.run_number,
        "materials_used": used_items,
        "total_cost": round(sum(i["cost"] for i in used_items), 2),
    }


# ============================================================================
# EVENTS ENDPOINTS
# ============================================================================

@app.get("/coo/events/status", tags=["Events"])
async def event_bus_status():
    """Get event bus status."""
    return {
        "event_bus_available": True,
        "redis_connected": event_bus.is_connected(),
        "redis_url_configured": bool(settings.redis_url)
    }


@app.post("/coo/events/receive", tags=["Events"])
async def receive_event(request: Request):
    """Receive events from other modules."""
    try:
        event_data = await request.json()
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    required = ["event_type", "source_module", "payload"]
    for field in required:
        if field not in event_data:
            raise HTTPException(status_code=400, detail=f"Missing: {field}")

    event_bus.handle_incoming_event(event_data)

    return {
        "success": True,
        "message": f"Event {event_data['event_type']} received",
        "event_id": event_data.get("event_id")
    }


@app.post("/coo/events/test", tags=["Events"])
async def test_event():
    """Test event bus by publishing a test event."""
    event_id = event_bus.publish(
        "test_event",
        {"message": "Test from COO module", "timestamp": datetime.utcnow().isoformat()},
        target_module=None
    )
    return {"success": True, "event_id": event_id}


@app.post("/coo/events/test-low", tags=["Events"])
async def test_low_alert(db: Session = Depends(get_db_session)):
    """Test inventory low alert flow to CMO."""
    # Find a low stock item
    low_stock_item = db.query(FinishedGoodsInventory).filter(
        FinishedGoodsInventory.is_low_stock == True,
        FinishedGoodsInventory.quantity_on_hand > 0
    ).first()

    if low_stock_item:
        item_name = f"{low_stock_item.product_title} - {low_stock_item.variant_title}"
        sku = low_stock_item.sku
        quantity = low_stock_item.quantity_on_hand
        item_id = low_stock_item.id
        reorder_point = low_stock_item.reorder_point
    else:
        item_name = "Test Product"
        sku = "TEST-SKU"
        quantity = 5
        item_id = 0
        reorder_point = 10

    event_id = publish_inventory_low(
        item_type="sub_assembly",
        item_id=item_id,
        item_name=item_name,
        current_quantity=quantity,
        reorder_point=reorder_point,
        sku=sku
    )

    return {
        "success": True,
        "event_id": event_id,
        "target": "cmo",
        "item": item_name,
        "quantity": quantity,
        "message": "Inventory low event sent to CMO"
    }


@app.post("/coo/events/test-critical", tags=["Events"])
async def test_critical_alert(db: Session = Depends(get_db_session)):
    """Test inventory critical alert flow to CEO."""
    # Find a low stock item to use as example
    low_stock_item = db.query(FinishedGoodsInventory).filter(
        FinishedGoodsInventory.quantity_on_hand <= 0
    ).first()

    if low_stock_item:
        item_name = f"{low_stock_item.product_title} - {low_stock_item.variant_title}"
        sku = low_stock_item.sku
        quantity = low_stock_item.quantity_on_hand
        item_id = low_stock_item.id
    else:
        item_name = "Test Product"
        sku = "TEST-SKU"
        quantity = -5
        item_id = 0

    event_id = publish_inventory_critical(
        item_type="sub_assembly",
        item_id=item_id,
        item_name=item_name,
        current_quantity=quantity,
        sku=sku
    )

    return {
        "success": True,
        "event_id": event_id,
        "item": item_name,
        "quantity": quantity,
        "message": "Inventory critical event sent to CEO"
    }


# ============================================================================
# DASHBOARD ENDPOINT
# ============================================================================

@app.get("/coo/dashboard", tags=["Dashboard"])
async def dashboard_summary(db: Session = Depends(get_db_session)):
    """Get COO dashboard summary."""
    # Raw materials stats
    total_materials = db.query(RawMaterial).filter(RawMaterial.is_active == True).count()
    low_stock_materials = db.query(RawMaterial).filter(
        RawMaterial.is_active == True,
        RawMaterial.is_low_stock == True
    ).count()

    # Production stats
    active_runs = db.query(ProductionRun).filter(ProductionRun.status == "in_progress").count()
    planned_runs = db.query(ProductionRun).filter(ProductionRun.status == "planned").count()

    # Finished goods stats
    total_finished = db.query(FinishedGoodsInventory).count()
    low_stock_finished = db.query(FinishedGoodsInventory).filter(
        FinishedGoodsInventory.is_low_stock == True
    ).count()

    # Unresolved alerts
    unresolved_alerts = db.query(COOAlert).filter(COOAlert.is_resolved == False).count()

    # Purchase order stats
    pending_pos = db.query(PurchaseOrder).filter(
        PurchaseOrder.status.in_([POStatus.DRAFT, POStatus.PENDING_APPROVAL, POStatus.APPROVED, POStatus.SENT])
    ).count()
    awaiting_approval = db.query(PurchaseOrder).filter(
        PurchaseOrder.status == POStatus.PENDING_APPROVAL
    ).count()

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "raw_materials": {
            "total": total_materials,
            "low_stock": low_stock_materials
        },
        "production": {
            "active_runs": active_runs,
            "planned_runs": planned_runs
        },
        "finished_goods": {
            "total_skus": total_finished,
            "low_stock": low_stock_finished
        },
        "purchase_orders": {
            "open": pending_pos,
            "awaiting_approval": awaiting_approval
        },
        "alerts": {
            "unresolved": unresolved_alerts
        }
    }


# ============================================================================
# ROOT
# ============================================================================

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint."""
    return {
        "module": "Dearborn AI COO",
        "description": "Operations module for inventory and production management",
        "version": "2.0.0",
        "endpoints": {
            "materials": "/coo/materials",
            "production": "/coo/production",
            "finished_goods": "/coo/finished-goods",
            "suppliers": "/coo/suppliers",
            "purchase_orders": "/coo/purchase-orders",
            "reorder": "/coo/reorder-suggestions",
            "alerts": "/coo/alerts",
            "dashboard": "/coo/dashboard"
        }
    }


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
