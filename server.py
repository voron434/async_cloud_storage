import os
import logging
import argparse

import asyncio
import aiofiles

from aiohttp import web


CHUNK_SIZE = 10**6  # ~100KB per iteration


async def archivate(request):
    archive_hash = request.match_info.get('archive_hash')

    filepath_to_be_zipped = os.path.join(request.app.source_path, archive_hash)
    if not os.path.exists(filepath_to_be_zipped):
        raise aiohttp.web.HTTPFound()

    response = web.StreamResponse()
    response.headers['Content-Disposition'] = f'attachment; filename="{archive_hash}.zip"'
    await response.prepare(request)

    zip_command = f"zip -r - {archive_hash}"
    zip_process = await asyncio.create_subprocess_shell(
        zip_command,
        cwd=request.app.source_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    try:
        while True:
            line = await zip_process.stdout.read(CHUNK_SIZE)
            if line == b'':
                break
            await response.write(line)
            if request.app.enable_throttling:
                await asyncio.sleep(1)
        if request.app.enable_logging:
            app.logger.info(f"Load sucess for request on {request.raw_path}")
    except asyncio.CancelledError:
        if request.app.enable_logging:
            app.logger.warning(f"Load interrupted for request on {request.raw_path}")
        zip_process.kill()
        await zip_process.communicate()
    finally:
        return response


async def handle_index_page(request):
    async with aiofiles.open('index.html', mode='r') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


def configure_app_with_argparse(app):
    parser = argparse.ArgumentParser(description='Cloud storage web-server')
    parser.add_argument('-p', '--source_path', help='Catalog where files are stored')
    parser.add_argument('-l', '--enable_logging', action='store_true', help='Enadles logging')
    parser.add_argument('-t', '--enable_throttling', action='store_true', help='Add server-side pauses for debugging')
    
    args = parser.parse_args()
    
    app.enable_throttling = args.enable_throttling
    app.enable_logging = args.enable_logging
    app.source_path = args.source_path or 'test_photos'

    logger = logging.getLogger("app")
    logger.setLevel(logging.INFO)
    app.logger = logger
    return app


if __name__ == '__main__':
    app = web.Application()
    app = configure_app_with_argparse(app)
    app.add_routes([
        web.get('/', handle_index_page),
        web.get('/archive/{archive_hash}/', archivate),
    ])
    web.run_app(app)
