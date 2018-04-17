FROM python:3.5

LABEL app.name="Memgator Bulk Download" \
      app.description="Bulk Retrieve TimeMaps In A Timely Manner" \
      app.license="MIT License" \
      app.license.url="https://github.com/N0taN3rd/memgatorBulkDownload/blob/master/LICENSE" \
      app.repo.url="https://github.com/N0taN3rd/memgatorBulkDownload" \
      app.maintainer="John Berlin <@johnaberlin>"

COPY . /



