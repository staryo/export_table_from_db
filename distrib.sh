PROJECT_DIRPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run \
    --rm \
    --workdir='/usr/src/myapp' \
    -v "${PROJECT_DIRPATH}:/usr/src/myapp" \
    python:3.8-bullseye bash -c "apt update ; apt install --yes unixodbc-dev;
                               pip3 install pyinstaller;
                               pip3 install -r requirements.txt;
                               pip3 install --upgrade snowflake-sqlalchemy
                               pip3 install pandas;
                               pyinstaller main.py \
                               --clean \
                               --onefile \
                               --name postgre_to_csv \
                               --distpath=dist/linux/ ;
                               chown -R ${UID} dist; "