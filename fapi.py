import uvicorn
from utilities import IP


if __name__ == "__main__":
    uvicorn.run("fat:app", host=f'{IP()}', port=5000, reload=True, access_log=False)
