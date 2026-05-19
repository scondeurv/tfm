import argparse

VALID_BURST_BACKEND_OPTIONS = ["rabbitmq", "redis", "redis-stream", "redis-list", "s3"]
DEFAULT_DOCKER_IMAGE = "burstcomputing/runtime-rust-burst:latest"

def add_burst_to_parser(parser):
    parser.add_argument("--granularity", type=int, required=False, help="Workers per Burst pack", default=None)
    parser.add_argument("--join", action=argparse.BooleanOptionalAction, required=False,
                        help="Join burst workers in same invoker", default=False)
    parser.add_argument("--backend", type=str, required=True, help="Burst communication backend",
                        choices=VALID_BURST_BACKEND_OPTIONS)
    parser.add_argument("--chunk-size", type=int, required=False, help="Chunk size for burst messages (in KB)",
                        default=None)

def add_openwhisk_to_parser(parser):
    parser.add_argument("--ow-host", type=str, required=True, help="Openwhisk host")
    parser.add_argument("--ow-port", type=int, required=True, help="Openwhisk port")
    parser.add_argument("--runtime-memory", type=int, required=False, help="Memory to allocate to the runtime (in MB)",
                        default=None)
    parser.add_argument("--custom-image", type=str, required=False, help="Tag of the docker custom image to use",
                        default=DEFAULT_DOCKER_IMAGE)
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction, help="Debug mode", default=False)


def try_or_except(parser):
    try:
        return parser.parse_args()
    except argparse.ArgumentError:
        parser.print_help()
        exit(1)
