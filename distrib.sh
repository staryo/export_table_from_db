PROJECT_DIRPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run \
    --rm \
    --workdir='/usr/src/myapp' \
    -v "${PROJECT_DIRPATH}:/usr/src/myapp" \
    python:3.8 bash -c "pip3 install pyinstaller;
                               pip3 install -r requirements.txt;
                               pyinstaller main_wip_imz.py \
                               --clean \
                               --onefile \
                               --name get_imz_wip \
                               --distpath=dist/linux/ ;
                               pyinstaller main.py \
                               --clean \
                               --onefile \
                               --name postgre_to_csv \
                               --distpath=dist/linux/ ;
                               chown -R ${UID} dist; "