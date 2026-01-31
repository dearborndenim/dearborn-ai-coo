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
    ShopifyAuth
)
from .event_bus import (
    event_bus, publish_inventory_low, publish_inventory_critical,
    publish_inventory_restocked, publish_production_complete
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
        "version": "1.0.0",
        "endpoints": {
            "materials": "/coo/materials",
            "production": "/coo/production",
            "finished_goods": "/coo/finished-goods",
            "suppliers": "/coo/suppliers",
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
