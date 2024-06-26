PROJECT_DIRPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run \
    --rm \
    --workdir='/usr/src/myapp' \
    -v "${PROJECT_DIRPATH}:/usr/src/myapp" \
    python:3.8 bash -c "apt update ; apt install --yes unixodbc-dev;
                               pip3 install pyinstaller;
                               pip3 install -r requirements.txt;
                               pyinstaller main_mssql.py \
                               --clean \
                               --onefile \
                               --name mssql_to_csv \
                               --distpath=dist/linux/ ;
                               chown -R ${UID} dist; "

