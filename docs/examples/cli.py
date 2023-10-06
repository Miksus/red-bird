import argparse

from pydantic import BaseModel, Field
from redbird.repos import SQLRepo

class Item(BaseModel):
    id: int
    name: str = Field(description="Name of the item", default="No name")

repo = SQLRepo(conn_string="sqlite:///example.db", table="items", model=Item, id_field="id", if_missing="create")

def create_item(**kwargs):
    "Create an item"
    item = Item(**kwargs)
    repo.add(item)

def read_item(item_id=None):
    "Read one item (or all)"
    if item_id is None:
        # Read all
        for item in repo:
            print(repr(item))
    else:
        # Read one
        item = repo[item_id]
        print(repr(item))

def update_item(item_id, **values):
    "Update one item"
    values = {key: value for key, value in values.items() if value is not None}
    repo[item_id] = values
    repo.filter_by(id=2).update(**values)

def delete_item(item_id):
    "Delete one item"
    del repo[item_id]

def add_model_to_parser(parser: argparse.ArgumentParser, model):
    "Add Pydantic model's attributes as arguments to the parser"
    fields = model.model_fields
    for name, field in fields.items():
        parser.add_argument(
            f"--{name}", 
            dest=name, 
            type=field.type_, 
            default=field.default,
            help=field.field_info.description,
        )

def parse_args(args=None):
    "Parse CLI arguments"
    parser = argparse.ArgumentParser(prog='Simple CRUD app')
    subparsers = parser.add_subparsers(dest='action')

    # Create subparsers for a single item
    parser_create = subparsers.add_parser("create", help="Create an item")
    parser_read = subparsers.add_parser("read", help="Read an item")
    parser_update = subparsers.add_parser("update", help="Update an item")
    parser_delete = subparsers.add_parser("delete", help="Delete an item")

    # Create subparsers for multiple items
    parser_read_all = subparsers.add_parser("read_all", help="Read items")

    # Add arguments
    parser_read.add_argument("item_id", nargs="?", help="ID of the item")
    for sub_parser in (parser_update, parser_delete):
        sub_parser.add_argument("item_id", help="ID of the item")

    add_model_to_parser(parser_create, model=Item)
    add_model_to_parser(parser_update, model=Item)

    return parser.parse_args(args)

def main(args=None):
    "A simple app to operate on a datastore"
    args = parse_args(args)
    args = vars(args)

    action = args.pop("action")
    func = {
        'create': create_item,
        'read': read_item,
        'update': update_item,
        'delete': delete_item,
    }[action]

    return func(**args)

if __name__ == "__main__":
    main()
