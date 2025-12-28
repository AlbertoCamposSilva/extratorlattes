from setuptools import setup, find_packages

setup (
    name='extrator-lattes',
    version='0.1.0',
    license='LGPL-3.0',
    author='Alberto de Campos e Silva',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author_email='silva@albertocampos.com.br',
    keywords='currículo lattes extrator dados pesquisa acadêmica',
    description='Ferramenta para extrair e analisar dados de currículos Lattes.',
    url='https://www.albertocampos.com.br/extrator-lattes',
    packages=find_packages(),
    install_requires=[
        'zeep',
        'pytz',
        'requests',
        'xmltodict',
        'pandas',
        'numpy',
        'xlsxwriter',
        'psycopg2-binary',
        'SQLAlchemy',
        'statsmodels',
        'Flask',
        'unidecode',
        'beautifulsoup4',
    ]
)