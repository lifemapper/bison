--------------------------------------------------
Requirements for Develop or Run of BISON data load
--------------------------------------------------

Python
======

Interpreter
***********
    * Python 3.6  
        module load opt-python
        python3.6 --version

Dependencies
*************
    * pip3.6 (needed for setuptools)
        python3.6 -m ensurepip --default-pip
        pip3 --version

        You are using pip version 9.0.1, however version 19.2.3 is available.
        You should consider upgrading via the 'pip install --upgrade pip' command.
    
        pip3 install --upgrade pip
    
    * setuptools (needed for requests)
        ARCHIVENAME=setuptools
        VERSION=41.2.0
        SRC_URL=https://files.pythonhosted.org/packages/d9/ca/7279974e489e8b65003fe618a1a741d6350227fa2bf48d16be76c7422423/$ARCHIVENAME-$VERSION.zip
        wget $SRC_URL
    
        unzip $ARCHIVENAME-$VERSION.zip
        cd $ARCHIVENAME-$VERSION
        module load opt-python
        $PYPATH setup.py install

    * unicodecsv 
        ARCHIVENAME=python-unicodecsv
        VERSION=0.14.1
        SRC_URL=https://github.com/jdunck/$ARCHIVENAME/archive/$VERSION.tar.gz
        OUTFILE=$ARCHIVENAME-$VERSION.tar.gz
        wget --output-document=$OUTFILE $SRC_URL

    * requests 2.22.0

        ARCHIVENAME=requests
        VERSION=2.22.0
        SRC_URL=https://github.com/kennethreitz/$ARCHIVENAME/archive/v$VERSION.tar.gz
        OUTFILE=$ARCHIVENAME-$VERSION.tar.gz
        wget --output-document=$OUTFILE $SRC_URL

Install dependencies for Rocks 7.0 (as root)::   
        PYVER=python3.6
        PYPATH=/opt/python/bin/$PYVER
        
        tar xzvf $OUTFILE
        cd $ARCHIVENAME-$VERSION
        
        module load opt-python
        $PYPATH setup.py install
