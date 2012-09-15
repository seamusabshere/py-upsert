from distutils.core import setup
import sys

sys.path.append('upsert')
import upsert

setup(name='upsert',
      version='0.0',
      author='Seamus Abshere',
      author_email='seamus@abshere.net',
      url='https://github.com/seamusabshere/py-upsert',
      # download_url='https://github.com/seamusabshere/py-upsert',
      description='Upsert for MySQL, PostgreSQL, SQLite3.',
      long_description=upsert.Upsert.__doc__,
      package_dir={'': 'upsert'},
      py_modules=['upsert'],
      provides=['upsert'],
      keywords='upsert sql merge',
      license='MIT',
      # classifiers=['Development Status :: 5 - Production/Stable',
      #              'Intended Audience :: Developers',
      #              'Natural Language :: English',
      #              'Operating System :: OS Independent',
      #              'Programming Language :: Python :: 2',
      #              'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
      #              'License :: OSI Approved :: GNU Affero General Public License v3',
      #              'Topic :: Internet',
      #              'Topic :: Internet :: WWW/HTTP',
      #              'Topic :: Scientific/Engineering :: GIS',
      #             ],
     )