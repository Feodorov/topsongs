# Top Songs
## Description
This project finds top 10 most popular songs from the top 50 longest sessions by track count in [Last.fm](http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html). Within a session each song starts within 20 minutes from a previous one.

## How to run
1. Build docker image:
```sh
docker build -t topsongs .
```
2. Run
```sh
docker run -it \
  -v /path/to/folder/with/input/data:/data \
  topsongs:latest \
  --input-file /data/userid-timestamp-artid-artname-traid-traname.tsv \
  --output-folder /data/output
```
`/path/to/folder/with/input/data` refers to a working folder with `userid-timestamp-artid-artname-traid-traname.tsv`.
Result will appear in the `./output` folder within the working folder.
`input-file` and `output-folder` should start with the mounted directory name (`/data` in this case)

## Assumptions
* This example is built to run with spark local mode. Master is hardcoded to `local[*]`
* There might be some docker-specific quirks related to running it on Windows. Unfortunately I don't have a Windows machine to check.
* Code is not optimized. Probably there is a faster way to do it.
* Sessions and songs are selected randomly in case of ties.
* Tracks without a title are not considered.

## Possible improvements
* Add remote cluster support
* More comprehensive test suite
* Set up linter checks on commit
* CI integration
