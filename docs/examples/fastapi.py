from fastapi import FastAPI
from pydantic import BaseModel
from redbird.repos import MemoryRepo

app = FastAPI()

class Item(BaseModel):
    id: int
    name: str

repo = MemoryRepo(model=Item, id_field="id")

@app.post("/items", description="Create an item")
def create_item(item: Item):
    repo.add(item)

@app.get("/items", description="Get all items")
def get_items():
    return repo.filter_by().all()

@app.get("/items/{item_id}", description="Get item by ID")
def get_item(item_id: int):
    return repo[item_id]

@app.patch("/items/{item_id}", description="Update an item")
def update_item(item_id: int, values: dict):
    repo[item_id] = values

@app.delete("/items/{item_id}", description="Delete an item")
def delete_item(item_id: int):
    del repo[item_id]
