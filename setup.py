from distutils.core import setup
import sys

sys.path.append('upsert')
import upsert

setup(name='upsert',
      packages=['upsert'],
      author='Seamus Abshere',
      author_email='seamus@abshere.net',
      url='https://github.com/seamusabshere/py-upsert',
      version='0.0.1',
      download_url='https://github.com/seamusabshere/py-upsert/zipball/v0.0.1',
      description='Upsert for MySQL, PostgreSQL, SQLite3.',
      long_description=upsert.Upsert.__doc__,
      keywords=['upsert', 'sql', 'merge'],
      license='MIT',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 2',
          'License :: OSI Approved :: MIT License',
          'Topic :: Database',
      ],
     )