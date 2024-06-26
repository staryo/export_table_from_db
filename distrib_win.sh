PROJECT_DIRPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run \
    --rm \
    --workdir='/src' \
    -v "${PROJECT_DIRPATH}:/src/" \
    tobix/pywine bash -c "wine pip install -r requirements.txt;
                          wine pip install pyinstaller ;
                          wine pyinstaller main_wip_imz.py \
                               --clean \
                               --distpath=dist/windows/ \
                               --name get_imz_wip \
                               --onefile -y;
                               chown -R ${UID} dist; "