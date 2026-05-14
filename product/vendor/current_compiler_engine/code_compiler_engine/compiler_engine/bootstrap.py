from compiler_engine.adapters.yaml_codec import dump_yaml_file, load_data_file
from compiler_engine.ports import DumpPort, LoadPort


def build_default_ports() -> dict[str, LoadPort | DumpPort]:
    return {
        'load_data': load_data_file,
        'dump_yaml': dump_yaml_file,
    }
