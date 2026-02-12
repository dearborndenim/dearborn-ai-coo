"""
Event Bus Client for COO Module

Handles inter-module communication via Redis pub/sub.
Publishes inventory alerts and receives events from other modules.
"""

import json
import os
import threading
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
import redis
from uuid import uuid4

from .db import SessionLocal, COOAlert, AlertSeverity
from .config import get_settings

settings = get_settings()


class COOOutboundEvent(str, Enum):
    """Events the COO module publishes"""
    INVENTORY_LOW = "inventory_low"
    INVENTORY_CRITICAL = "inventory_critical"
    INVENTORY_RESTOCKED = "inventory_restocked"
    PRODUCTION_DELAYED = "production_delayed"
    PRODUCTION_COMPLETE = "production_complete"
    MATERIAL_REORDER_NEEDED = "material_reorder_needed"


class COOOutboundEvent2(str, Enum):
    """Additional outbound events"""
    INVENTORY_COST_UPDATE = "inventory_cost_update"
    PRODUCTION_COSTS_ACTUAL = "production_costs_actual"
    PO_APPROVAL_NEEDED = "po_approval_needed"


class COOInboundEvent(str, Enum):
    """Events the COO module listens for"""
    APPROVAL_DECIDED = "approval_decided"
    CAMPAIGN_PLANNED = "campaign_planned"
    PRIORITY_CHANGED = "priority_changed"
    BUDGET_ALLOCATED = "budget_allocated"
    CAPACITY_CHECK_REQUEST = "capacity_check_request"
    TECH_PACK_READY = "tech_pack_ready"
    PRODUCT_APPROVED_FOR_PRODUCTION = "product_approved_for_production"


class EventBus:
    """Redis pub/sub event bus for inter-module communication."""

    def __init__(self):
        self.redis_url = settings.redis_url
        self._client: Optional[redis.Redis] = None

    @property
    def client(self) -> Optional[redis.Redis]:
        """Lazy initialization of Redis client."""
        if not self.redis_url:
            return None

        if self._client is None:
            try:
                self._client = redis.from_url(
                    self.redis_url,
                    decode_responses=True,
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
                self._client.ping()
            except Exception as e:
                print(f"Failed to connect to Redis: {e}")
                self._client = None

        return self._client

    def is_connected(self) -> bool:
        """Check if Redis is connected."""
        if not self.client:
            return False
        try:
            self.client.ping()
            return True
        except:
            return False

    def publish(
        self,
        event_type: COOOutboundEvent,
        payload: Dict[str, Any],
        target_module: Optional[str] = None
    ) -> Optional[str]:
        """Publish an event to the event bus."""
        event_id = str(uuid4())

        event = {
            "event_id": event_id,
            "event_type": event_type.value if isinstance(event_type, COOOutboundEvent) else event_type,
            "source_module": "coo",
            "target_module": target_module,
            "payload": payload,
            "timestamp": datetime.utcnow().isoformat()
        }

        # Log to database
        self._log_event_to_db(event, direction="outbound")

        # Try Redis
        redis_delivered = False
        if self.client:
            try:
                channel = f"dearborn:events:{target_module}" if target_module else "dearborn:events:broadcast"
                receivers = self.client.publish(channel, json.dumps(event))
                print(f"Published event {event_type} to {channel} ({receivers} receivers)")
                if receivers > 0:
                    return event_id
                redis_delivered = False
            except Exception as e:
                print(f"Failed to publish to Redis: {e}")

        # HTTP fallback when Redis has no subscribers or is unavailable
        try:
            import httpx

            # CEO fallback
            if target_module == "ceo" and settings.ceo_api_url:
                response = httpx.post(
                    f"{settings.ceo_api_url}/ceo/approvals",
                    json={
                        "requesting_module": "coo",
                        "request_type": event_type.value if isinstance(event_type, COOOutboundEvent) else event_type,
                        "title": payload.get("title", f"COO: {event_type}"),
                        "description": payload.get("message", ""),
                        "payload": payload,
                        "risk_level": "medium" if "critical" in str(event_type).lower() else "low"
                    },
                    timeout=10.0
                )
                if response.status_code == 200:
                    print(f"Published event {event_type} to CEO via HTTP fallback")

            # CMO fallback (serverless - can't run Redis listener)
            if target_module == "cmo" and settings.cmo_api_url:
                response = httpx.post(
                    f"{settings.cmo_api_url}/api/events/webhook",
                    json=event,
                    timeout=10.0
                )
                if response.status_code == 200:
                    print(f"Published event {event_type} to CMO via HTTP webhook")

            # CFO fallback
            if target_module == "cfo" and settings.cfo_api_url:
                response = httpx.post(
                    f"{settings.cfo_api_url}/cfo/events/receive",
                    json=event,
                    timeout=10.0
                )
                if response.status_code == 200:
                    print(f"Published event {event_type} to CFO via HTTP fallback")

            # CDO fallback
            if target_module == "cdo" and settings.cdo_api_url:
                response = httpx.post(
                    f"{settings.cdo_api_url}/cdo/events/webhook",
                    json=event,
                    timeout=10.0
                )
                if response.status_code == 200:
                    print(f"Published event {event_type} to CDO via HTTP fallback")

        except Exception as e:
            print(f"Failed to publish via HTTP fallback: {e}")

        return event_id

    def _log_event_to_db(self, event: Dict[str, Any], direction: str = "outbound"):
        """Log event to database for audit trail."""
        try:
            db = SessionLocal()
            from .db import COOEvent
            db_event = COOEvent(
                direction=direction,
                other_module=event.get("target_module") or event.get("source_module", "broadcast"),
                event_type=event["event_type"],
                payload=event,
                status="sent" if direction == "outbound" else "received"
            )
            db.add(db_event)
            db.commit()
            db.close()
        except Exception as e:
            print(f"Failed to log event to DB: {e}")

    def handle_incoming_event(self, event: Dict[str, Any]):
        """Process an incoming event."""
        event_type = event.get("event_type")
        source = event.get("source_module")
        payload = event.get("payload", {})

        print(f"Received event {event_type} from {source}")
        self._log_event_to_db(event, direction="inbound")

        try:
            if event_type == COOInboundEvent.APPROVAL_DECIDED.value:
                self._handle_approval_decided(payload)
            elif event_type == COOInboundEvent.CAMPAIGN_PLANNED.value:
                self._handle_campaign_planned(payload)
            elif event_type == COOInboundEvent.PRIORITY_CHANGED.value:
                self._handle_priority_changed(payload)
            elif event_type == COOInboundEvent.BUDGET_ALLOCATED.value:
                self._handle_budget_allocated(payload)
            elif event_type == COOInboundEvent.CAPACITY_CHECK_REQUEST.value:
                self._handle_capacity_check_request(payload)
            elif event_type == COOInboundEvent.TECH_PACK_READY.value:
                self._handle_tech_pack_ready(payload)
            elif event_type == COOInboundEvent.PRODUCT_APPROVED_FOR_PRODUCTION.value:
                self._handle_product_approved_for_production(payload)
            else:
                print(f"Unknown event type: {event_type}")
        except Exception as e:
            print(f"Error handling event {event_type}: {e}")

    def _handle_approval_decided(self, payload: Dict[str, Any]):
        """Handle approval decision from CEO."""
        status = payload.get("status")
        requesting_module = payload.get("requesting_module")

        if requesting_module != "coo":
            return

        db = SessionLocal()
        try:
            alert = COOAlert(
                severity=AlertSeverity.INFO if status == "approved" else AlertSeverity.WARNING,
                category="approval",
                title=f"Request {status.title()}",
                message=f"CEO has {status} the COO request"
            )
            db.add(alert)
            db.commit()
        finally:
            db.close()

    def _handle_campaign_planned(self, payload: Dict[str, Any]):
        """Handle campaign planned from CMO - check if we have inventory."""
        product_ids = payload.get("product_ids", [])
        print(f"CMO planning campaign for products: {product_ids}")

        if not product_ids:
            return

        db = SessionLocal()
        try:
            from .db import FinishedGoodsInventory
            low_items = []
            for pid in product_ids:
                items = db.query(FinishedGoodsInventory).filter(
                    FinishedGoodsInventory.shopify_product_id == str(pid),
                    FinishedGoodsInventory.is_low_stock == True
                ).all()
                for item in items:
                    low_items.append(item)
                    publish_inventory_low(
                        item_type="finished_goods",
                        item_id=item.id,
                        item_name=f"{item.product_title} - {item.variant_title}",
                        current_quantity=item.quantity_on_hand,
                        reorder_point=item.reorder_point,
                        sku=item.sku
                    )

            if low_items:
                alert = COOAlert(
                    severity=AlertSeverity.WARNING,
                    category="inventory",
                    title="Campaign Stock Warning",
                    message=f"CMO campaign targets {len(product_ids)} products; {len(low_items)} variants are low stock"
                )
                db.add(alert)
                db.commit()
        finally:
            db.close()

    def _handle_priority_changed(self, payload: Dict[str, Any]):
        """Handle priority change from CEO."""
        db = SessionLocal()
        try:
            alert = COOAlert(
                severity=AlertSeverity.WARNING,
                category="priority",
                title="Business Priority Changed",
                message=f"CEO changed priority: {payload.get('priority_type')} -> {payload.get('new_priority')}"
            )
            db.add(alert)
            db.commit()
        finally:
            db.close()

    def _handle_budget_allocated(self, payload: Dict[str, Any]):
        """Handle budget allocation from CEO."""
        module = payload.get("module")
        amount = payload.get("amount", 0)
        category = payload.get("category", "general")

        if module and module != "coo":
            return

        db = SessionLocal()
        try:
            alert = COOAlert(
                severity=AlertSeverity.INFO,
                category="budget",
                title="Budget Allocated",
                message=f"${amount:,.2f} allocated for {category}"
            )
            db.add(alert)
            db.commit()
        finally:
            db.close()

    def _handle_capacity_check_request(self, payload: Dict[str, Any]):
        """Handle capacity check request from CDO for new product concepts."""
        validation_request_id = payload.get("validation_request_id")
        quantity_needed = payload.get("quantity_needed", 0)

        db = SessionLocal()
        try:
            from .db import ProductionRun
            active_runs = db.query(ProductionRun).filter(
                ProductionRun.status == "in_progress"
            ).count()
            planned_runs = db.query(ProductionRun).filter(
                ProductionRun.status == "planned"
            ).count()

            max_concurrent = 5
            has_capacity = (active_runs + planned_runs) < max_concurrent
            available_slots = max(0, max_concurrent - active_runs - planned_runs)

            self.publish(
                "capacity_check_response",
                {
                    "validation_request_id": validation_request_id,
                    "approved": has_capacity,
                    "active_runs": active_runs,
                    "planned_runs": planned_runs,
                    "available_slots": available_slots,
                    "summary": f"Capacity {'available' if has_capacity else 'full'}: {active_runs} active, {planned_runs} planned, {available_slots} slots open"
                },
                target_module="cdo"
            )

            alert = COOAlert(
                severity=AlertSeverity.INFO if has_capacity else AlertSeverity.WARNING,
                category="capacity",
                title=f"Capacity Check: {'Available' if has_capacity else 'Full'}",
                message=f"CDO requested capacity for validation {validation_request_id}. {active_runs} active, {planned_runs} planned runs."
            )
            db.add(alert)
            db.commit()
        except Exception as e:
            print(f"Error handling capacity check: {e}")
        finally:
            db.close()

    def _handle_tech_pack_ready(self, payload: Dict[str, Any]):
        """Handle tech pack ready from CDO - create sourcing alert and draft POs from BOM."""
        tech_pack_id = payload.get("tech_pack_id")
        tech_pack_number = payload.get("tech_pack_number", "Unknown")
        bom = payload.get("bom", [])

        db = SessionLocal()
        try:
            # Create alert for operations team
            alert = COOAlert(
                severity=AlertSeverity.WARNING,
                category="sourcing",
                title=f"Sourcing Needed: {tech_pack_number}",
                message=f"Tech pack {tech_pack_number} is ready. {len(bom)} materials need sourcing."
            )
            db.add(alert)

            # Auto-create draft POs from BOM if materials are provided
            if bom:
                from .db import PurchaseOrder, PurchaseOrderItem, Supplier, RawMaterial
                estimated_units = payload.get("estimated_units", 500)
                waste_factor = 1.05  # 5% waste

                # Find a default supplier or use first available
                supplier = db.query(Supplier).first()
                if supplier:
                    # Create one PO for all materials from this tech pack
                    total_po_cost = 0
                    bom_details = []

                    for item in bom:
                        material_name = item.get("material", "Unknown material")
                        quantity_per_unit = item.get("quantity", 0)
                        unit_cost = item.get("unit_cost", 0)
                        total_qty = quantity_per_unit * estimated_units * waste_factor
                        item_total = total_qty * unit_cost
                        total_po_cost += item_total

                        bom_details.append(
                            f"- {material_name}: {total_qty:.2f} units @ ${unit_cost:.2f}/unit = ${item_total:.2f}"
                        )

                    po = PurchaseOrder(
                        supplier_id=supplier.id,
                        status="draft",
                        notes=f"Auto-generated from tech pack {tech_pack_number}\nBOM:\n" + "\n".join(bom_details),
                        total=round(total_po_cost, 2),
                    )
                    db.add(po)
                    db.flush()

                    # Try to create PO items for materials that exist in the system
                    for item in bom:
                        material_name = item.get("material", "Unknown material")
                        quantity_per_unit = item.get("quantity", 0)
                        unit_cost = item.get("unit_cost", 0)
                        total_qty = quantity_per_unit * estimated_units * waste_factor

                        # Try to find existing material by name
                        material = db.query(RawMaterial).filter(
                            RawMaterial.name.ilike(f"%{material_name}%")
                        ).first()

                        if material:
                            po_item = PurchaseOrderItem(
                                purchase_order_id=po.id,
                                material_id=material.id,
                                quantity=round(total_qty, 2),
                                unit_cost=unit_cost,
                                total_cost=round(total_qty * unit_cost, 2),
                            )
                            db.add(po_item)

                # Publish sourcing cost estimate to CFO
                total_material_cost = sum(
                    (item.get("quantity", 0) * estimated_units * waste_factor * item.get("unit_cost", 0))
                    for item in bom
                )
                self.publish(
                    "sourcing_cost_estimate",
                    {
                        "tech_pack_id": tech_pack_id,
                        "tech_pack_number": tech_pack_number,
                        "estimated_units": estimated_units,
                        "total_material_cost": round(total_material_cost, 2),
                        "bom_items": len(bom),
                        "title": f"Sourcing Cost: {tech_pack_number}",
                        "message": f"Estimated material cost: ${total_material_cost:,.2f} for {estimated_units} units",
                    },
                    target_module="cfo"
                )

            db.commit()
        except Exception as e:
            print(f"Error handling tech_pack_ready: {e}")
            db.rollback()
        finally:
            db.close()

    def _handle_product_approved_for_production(self, payload: Dict[str, Any]):
        """Handle product approved for production from CDO - submit draft POs."""
        pipeline_number = payload.get("pipeline_number", "Unknown")
        tech_pack_id = payload.get("tech_pack_id")

        db = SessionLocal()
        try:
            # Find draft POs associated with this tech pack
            from .db import PurchaseOrder
            draft_pos = []
            if tech_pack_id:
                draft_pos = db.query(PurchaseOrder).filter(
                    PurchaseOrder.status == "draft",
                    PurchaseOrder.notes.contains(str(tech_pack_id))
                ).all()

            submitted_count = 0
            for po in draft_pos:
                po.status = "pending_approval"
                submitted_count += 1

            # Create production run entry
            from .db import ProductionRun
            run = ProductionRun(
                product_name=payload.get("title", f"Pipeline {pipeline_number}"),
                status="planned",
                quantity_planned=payload.get("estimated_units", 500),
                notes=f"Auto-created from pipeline {pipeline_number}",
            )
            db.add(run)

            alert = COOAlert(
                severity=AlertSeverity.INFO,
                category="production",
                title=f"Production Approved: {pipeline_number}",
                message=f"Product from pipeline {pipeline_number} approved. {submitted_count} POs submitted, production run created."
            )
            db.add(alert)

            db.commit()
        except Exception as e:
            print(f"Error handling product_approved_for_production: {e}")
            db.rollback()
        finally:
            db.close()

    def start_listener(self):
        """Start background Redis listener thread for incoming events."""
        if not self.client:
            print("Redis not connected - COO listener not started")
            return

        def _listen():
            try:
                pubsub = self.client.pubsub()
                pubsub.subscribe("dearborn:events:coo", "dearborn:events:broadcast")
                print("COO Redis listener started on dearborn:events:coo + broadcast")
                for message in pubsub.listen():
                    if message["type"] == "message":
                        try:
                            event = json.loads(message["data"])
                            if event.get("source_module") != "coo":
                                self.handle_incoming_event(event)
                        except Exception as e:
                            print(f"COO listener error: {e}")
            except Exception as e:
                print(f"COO Redis listener failed: {e}")

        thread = threading.Thread(target=_listen, daemon=True)
        thread.start()

    def disconnect(self):
        """Disconnect from Redis."""
        if self._client:
            self._client.close()
            self._client = None


# Singleton instance
event_bus = EventBus()


# Convenience functions

def publish_inventory_low(
    item_type: str,  # 'raw_material' or 'finished_goods'
    item_id: int,
    item_name: str,
    current_quantity: float,
    reorder_point: float,
    sku: str = None
) -> Optional[str]:
    """Publish inventory low alert."""
    return event_bus.publish(
        COOOutboundEvent.INVENTORY_LOW,
        {
            "item_type": item_type,
            "item_id": item_id,
            "item_name": item_name,
            "sku": sku,
            "current_quantity": current_quantity,
            "reorder_point": reorder_point,
            "title": f"Low Inventory: {item_name}",
            "message": f"{item_name} ({sku}) has {current_quantity} units remaining (reorder point: {reorder_point})"
        },
        target_module="cmo"  # Alert CMO to pause marketing
    )


def publish_inventory_critical(
    item_type: str,
    item_id: int,
    item_name: str,
    current_quantity: float,
    sku: str = None
) -> Optional[str]:
    """Publish inventory critical alert."""
    return event_bus.publish(
        COOOutboundEvent.INVENTORY_CRITICAL,
        {
            "item_type": item_type,
            "item_id": item_id,
            "item_name": item_name,
            "sku": sku,
            "current_quantity": current_quantity,
            "title": f"CRITICAL: {item_name} Nearly Out of Stock",
            "message": f"{item_name} ({sku}) has only {current_quantity} units remaining!"
        },
        target_module="ceo"  # Alert CEO for critical decisions
    )


def publish_inventory_restocked(
    item_type: str,
    item_id: int,
    item_name: str,
    new_quantity: float,
    sku: str = None
) -> Optional[str]:
    """Publish inventory restocked notification."""
    return event_bus.publish(
        COOOutboundEvent.INVENTORY_RESTOCKED,
        {
            "item_type": item_type,
            "item_id": item_id,
            "item_name": item_name,
            "sku": sku,
            "new_quantity": new_quantity,
            "title": f"Restocked: {item_name}",
            "message": f"{item_name} ({sku}) has been restocked with {new_quantity} units"
        },
        target_module="cmo"  # Alert CMO to resume marketing
    )


def publish_production_complete(
    run_id: int,
    run_number: str,
    product_name: str,
    quantity_completed: int
) -> Optional[str]:
    """Publish production complete notification."""
    return event_bus.publish(
        COOOutboundEvent.PRODUCTION_COMPLETE,
        {
            "run_id": run_id,
            "run_number": run_number,
            "product_name": product_name,
            "quantity_completed": quantity_completed,
            "title": f"Production Complete: {product_name}",
            "message": f"Production run {run_number} completed: {quantity_completed} units of {product_name}"
        },
        target_module="cmo"  # Alert CMO that product is available
    )


def publish_production_costs(
    run_id: int,
    run_number: str,
    product_name: str,
    total_material_cost: float,
    cost_per_unit: float,
    quantity_completed: int
) -> Optional[str]:
    """Publish actual production costs to CFO."""
    return event_bus.publish(
        "production_costs_actual",
        {
            "run_id": run_id,
            "run_number": run_number,
            "product_name": product_name,
            "total_material_cost": total_material_cost,
            "cost_per_unit": cost_per_unit,
            "quantity_completed": quantity_completed,
            "title": f"Production Costs: {run_number}",
            "message": f"Run {run_number}: ${total_material_cost:.2f} total, ${cost_per_unit:.2f}/unit for {quantity_completed} units"
        },
        target_module="cfo"
    )


def publish_po_approval_needed(
    po_id: int,
    po_number: str,
    supplier_name: str,
    total: float
) -> Optional[str]:
    """Request CEO approval for a purchase order."""
    return event_bus.publish(
        "po_approval_needed",
        {
            "po_id": po_id,
            "po_number": po_number,
            "supplier_name": supplier_name,
            "total": total,
            "title": f"PO Approval: {po_number} - ${total:,.2f}",
            "message": f"Purchase order {po_number} for {supplier_name} totaling ${total:,.2f} needs approval"
        },
        target_module="ceo"
    )
