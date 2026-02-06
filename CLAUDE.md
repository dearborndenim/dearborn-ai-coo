# CLAUDE.md - Dearborn AI COO Module

Operations management: inventory, production runs, suppliers, purchase orders, and material reservations.

## Architecture

- **Framework:** FastAPI (Python)
- **Database:** PostgreSQL (schema: `coo`)
- **Event Bus:** Redis pub/sub (sync, threading-based)
- **Port:** 8080 (configurable via `PORT`)
- **Deploy:** Railway

## API Endpoints

### Health
- `GET /health` - Basic health check
- `GET /health/detailed` - DB + Redis connectivity check

### Dashboard
- `GET /coo/dashboard` - Operations overview (production, inventory, PO stats)

### Production Runs (`/coo/production`)
- `GET /coo/production` - List runs (filter: `?status=active`)
- `POST /coo/production` - Create run
- `GET /coo/production/{id}` - Run detail
- `POST /coo/production/{id}/start` - Start production
- `POST /coo/production/{id}/stage` - Advance stage
- `POST /coo/production/{id}/complete` - Complete run (updates inventory)
- `GET /coo/production/{id}/costs` - Material cost breakdown
- `POST /coo/production/{id}/publish-costs` - Send costs to CFO
- `POST /coo/production/{id}/use-materials` - Convert reserved materials to used

### Materials / Inventory (`/coo/materials`)
- `GET /coo/materials` - List materials (filter: `?low_stock=true`)
- `POST /coo/materials` - Create material
- `GET /coo/materials/{id}` - Material detail
- `PUT /coo/materials/{id}` - Update material
- `POST /coo/materials/{id}/transaction` - Record stock movement
- `POST /coo/materials/{id}/set-reorder` - Set reorder params
- `POST /coo/materials/reserve` - Reserve materials for production
- `POST /coo/materials/release` - Release reservation

### Finished Goods (`/coo/finished-goods`)
- `GET /coo/finished-goods` - List finished goods
- `POST /coo/finished-goods` - Create finished good
- `POST /coo/finished-goods/{id}/transaction` - Stock movement

### Suppliers (`/coo/suppliers`)
- `GET /coo/suppliers` - List suppliers
- `POST /coo/suppliers` - Create supplier

### Purchase Orders (`/coo/purchase-orders`)
- `GET /coo/purchase-orders` - List POs (filter: `?status=draft`)
- `POST /coo/purchase-orders` - Create PO (auto-approves under $500)
- `GET /coo/purchase-orders/{id}` - PO detail with items
- `POST /coo/purchase-orders/{id}/approve` - Approve PO
- `POST /coo/purchase-orders/{id}/send` - Mark as sent
- `POST /coo/purchase-orders/{id}/receive` - Receive goods (updates inventory)
- `POST /coo/purchase-orders/{id}/cancel` - Cancel PO

### Reorder Automation (`/coo/reorder-suggestions`)
- `GET /coo/reorder-suggestions` - Velocity-based suggestions with urgency
- `POST /coo/reorder-suggestions/generate-pos` - Auto-create PO drafts by supplier

## Event Bus

### Publishes:
| Event | Target | Trigger |
|-------|--------|---------|
| `inventory_low` | CMO | Material below reorder point |
| `inventory_critical` | CEO | Material critically low |
| `inventory_restocked` | CMO | Material restocked |
| `production_complete` | CMO | Production run completed |
| `production_costs_actual` | CFO | Cost data published |
| `po_approval_needed` | CEO | PO above auto-approve threshold |
| `capacity_check_response` | CDO | Response to capacity request |

### Subscribes to:
| Event | Source | Action |
|-------|--------|--------|
| `approval_decided` | CEO | Creates COOAlert |
| `campaign_planned` | CMO | Checks inventory for campaign products |
| `priority_changed` | CEO | Creates warning alert |
| `budget_allocated` | CEO | Creates budget alert |
| `capacity_check_request` | CDO | Returns capacity info |

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | `postgresql://localhost:5432/dearborn` | PostgreSQL |
| `REDIS_URL` | Yes | `redis://localhost:6379` | Redis event bus |
| `PORT` | No | 8080 | Server port |
| `ALLOWED_ORIGINS` | No | `""` | CORS origins |
| `CEO_API_URL` | No | `""` | CEO for HTTP fallback |
| `CFO_API_URL` | No | `""` | CFO for HTTP fallback |
| `CMO_API_URL` | No | `""` | CMO webhook delivery |
| `CDO_API_URL` | No | `""` | CDO for HTTP fallback |
| `SHOPIFY_STORE` | No | `dearborndenim.myshopify.com` | Shopify store |
| `SHOPIFY_ACCESS_TOKEN` | No | | Shopify API token |

## File Structure

```
src/
  config.py      - Pydantic settings
  db.py          - SQLAlchemy models (Material, ProductionRun, PurchaseOrder, etc.)
  event_bus.py   - Redis pub/sub + HTTP fallback (threading)
  server.py      - FastAPI app + all route handlers
```
