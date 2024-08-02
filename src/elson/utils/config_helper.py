import yaml, os, json
from elson.utils.exceptions import YamlNotFoundError, ParseYamlError, EnvSwitchError, EnvUtilError


__helper__ = """
Note:
    you can get you you env configration by using this util lib
eg: contents in '/Workspace/ETL/common_module/config/dev_config.yaml'
    ETLADLDBServer:
        driver: "{ODBC Driver 17 for SQL Server}"
    demo as below:
    +---------------------------------------------------+
    |>>> from etl_framework.config_helper import get_env|
    |    config = get_env()                             |
    |    driver = config.ETLADLDBServer.driver          |
    |    print(driver)                                  |
    +---------------------------------------------------+
    |... "{ODBC Driver 17 for SQL Server}"              |
    +---------------------------------------------------+
"""
class MultiLevelDictToClass(object):
    def __init__(self, *args):
        for arg in args:
            for k, v in arg.items():
                if isinstance(v, dict):
                    self.__dict__[k] = MultiLevelDictToClass(v)
                else:
                    self.__dict__[k] = v

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)


def get_config_yaml():
    env = os.getenv("ENV", default="DEV")
    config_path = f"/Workspace/ETL/common_module/config/{env.lower()}_config.yaml"
    if env.lower() in ["dev", "sat", "prod", "debug"]:
        if not os.path.exists(config_path):
            return YamlNotFoundError(message=f"{config_path} not found",
                                     error_caught="this is a message from func get_config_yaml()", )
        try:
            with open(config_path, mode="r", encoding="UTF-8") as file:
                env_objs = yaml.load(file, Loader=yaml.FullLoader)
            return env_objs
        except Exception as e:
            return ParseYamlError(message=f"Error raised when parsing {config_path}",
                                  error_caught=e, )
    else:
        return EnvSwitchError(message=f"ENV in os should be one of them: ('dev', 'sat', 'prod', 'debug') current: {env}",
                              error_caught="this is a message from func get_config_yaml()")


def get_env():
    env_objs = get_config_yaml()
    if issubclass(type(env_objs), EnvUtilError):
        return env_objs
    return MultiLevelDictToClass(env_objs)

# if __name__ == '__main__':
#     env_objs = get_config_yaml()
#     info = get_env()
#     print(str(info))