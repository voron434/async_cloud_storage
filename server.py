import os
import logging
import argparse

import asyncio
import aiofiles

from aiohttp import web

CHUNK_SIZE = 10**6  # ~100KB per iteration

logger = logging.getLogger("app")


async def archivate(request):
    archive_hash = request.match_info['archive_hash']

    filepath_to_be_zipped = os.path.join(request.app.source_path, archive_hash)
    if not os.path.exists(filepath_to_be_zipped):
        raise aiohttp.web.HTTPNotFound()

    response = web.StreamResponse()
    response.headers['Content-Disposition'] = f'attachment; filename="{archive_hash}.zip"'
    await response.prepare(request)

    zip_command = f"zip -r - {archive_hash}"
    zip_process = await asyncio.create_subprocess_exec(
        *zip_command.split(" "),
        cwd=request.app.source_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    try:
        while True:
            line = await zip_process.stdout.read(CHUNK_SIZE)
            if not line:
                break
            await response.write(line)
            if request.app.enable_throttling:
                await asyncio.sleep(1)
        logger.info(f"Load sucess for request on {request.raw_path}")
    except asyncio.CancelledError:
        logger.warning(f"Load interrupted for request on {request.raw_path}")
        zip_process.kill()
        await zip_process.communicate()
        raise
    return response


async def handle_index_page(request):
    async with aiofiles.open('index.html', mode='r') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


def create_argparser():
    parser = argparse.ArgumentParser(description='Cloud storage web-server')
    parser.add_argument('-p', '--source_path', default='test_photos', help='Catalog where files are stored')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='Enadles logging. Set -v for debug, -vvvvv for critical.')
    parser.add_argument('-t', '--enable_throttling', action='store_true', help='Add server-side pauses for debugging')
    return parser


if __name__ == '__main__':
    parser = create_argparser()
    args = parser.parse_args()
    
    formatter = logging.Formatter('%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(args.verbose * 10)

    app = web.Application()

    app.enable_throttling = args.enable_throttling
    app.source_path = args.source_path
    
    app.add_routes([
        web.get('/', handle_index_page),
        web.get('/archive/{archive_hash}/', archivate),
    ])
    web.run_app(app)
