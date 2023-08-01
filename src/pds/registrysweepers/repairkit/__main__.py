from pds.registrysweepers.repairkit import run
from pds.registrysweepers.utils import parse_args

args = parse_args(description='sweep through the registry documents and fix common errors')

run(base_url=args.base_URL,
    username=args.username,
    password=args.password,
    verify_host_certs=not args.insecure,
    log_level=args.log_level,
    log_filepath=args.log_file)
