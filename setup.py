from setuptools import setup

extras_require = {"celery": ["celery", "redis"]}

extras_require["viz"] = ["adage[viz]>=0.10.3", "pydotplus>=2.0.0"]

extras_require["lint"] = [
    "pyflakes",
    "isort",
    "black[jupyter]>=22.1.0",
]

extras_require["develop"] = [
    *extras_require["viz"],
    "pre-commit",
    "pytest>=6.0.0",
    "pytest-cov>=2.5.1",
]

extras_require["all"] = sorted(set(sum(extras_require.values(), [])))

setup(extras_require=extras_require)
