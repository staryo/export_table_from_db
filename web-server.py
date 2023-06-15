from datetime import datetime
from typing import Union

import pandas as pd
import uvicorn
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from main import read_config, script


class Item(BaseModel):
    title: str
    timestamp: datetime
    description: Union[str, None] = None


app = FastAPI()


@app.get("/ca/{config_name}")
async def api_data(config_name, request: Request):
    parameters = dict(request.query_params)
    config = read_config(f'{config_name}.yml')
    print(config['query'].format(**parameters))
    result = script(config['db'], config['query'].format(**parameters))

    json_compatible_item_data = jsonable_encoder(
        [{k: v for k, v in m.items() if pd.notnull(v)} for m in
         result.to_dict(orient='records')]
    )

    return JSONResponse(json_compatible_item_data)

if __name__ == "__main__":
    server_config = read_config('server.yml')
    uvicorn.run(
        app,
        host=server_config['host'],
        port=server_config['port']
    )
