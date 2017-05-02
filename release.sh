#!/usr/bin/env bash

set -ex

# Trying to install it for the user so he doesn't have to bother with installing stuff by hand
sudo apt-get -qq install xmlstarlet maven

# Global default vars used in this script
######################################################################################
BASE_DIR=$PWD
WORK_DIR=/tmp/elephant-bird_release
COMMAND="release"
ACTUAL_VERSION=$(xmlstarlet sel -t -v "/_:project/_:version" pom.xml)
RELEASE_VERSION=$(echo $ACTUAL_VERSION|sed 's/-SNAPSHOT//')
BASE_BRANCH=$(git rev-parse --abbrev-ref HEAD)
DIRTY_SCM=false
GIT_REPO=git@github.com:twitter/elephant-bird.git
THRIFT7_PATH="/tmp/thrift7"
THRIFT9_PATH="/tmp/thrift9"
HADOOP_LZO_PATH="/tmp/hadoop-lzo-native"
PROTOBUF_PATH="/tmp/protobuf"

######################################################################################

while [[ $# > 1 ]]; do
key="$1"

case $key in
    -c|--command)
      COMMAND="$2"
      ;;
    -r|--release-version)
      RELEASE_VERSION="$2"
      ;;
    -n|--next-version)
      NEXT_DEV_VERSION="$2"
      ;;
    -d|--dirty-scm)
      DIRTY_SCM="$2"
      ;;
    *)
      echo "Unknown parameter $key"
      echo "Usage: ./release.sh [OPTION]
-c,--comand commandName
    possible values test|install|deploy|release
-r,--release-version versionNumber
    will be used to deploy the artifacts and make the release branch
-n,--next-version versionNumber
    will be used as the new version after the release
-d,--dirty-scm true/false
    false by default, all changes must be in sync with origin"
      exit 1
      ;;
esac
shift 2
done

#######################################################################################################################
#######################################################################################################################
#######################################################################################################################

function checkNoUncommitedChanges {
  if [ "$DIRTY_SCM" == "false" ]; then
    echo "Checking that there are no uncommited changes"
    git diff-index --quiet origin/$BASE_BRANCH --
    local RET=$?
    if [ $RET != 0 ]; then
      echo "You have uncommited changes, please commit and push to origin everything before deploying the doc."
      exit $RET;
    fi;
  fi;
}

# We don't care here about updating dependencies versions as we use the project version for dependencies between modules
function updateVersions {
  local PROJECT=$1
  local PROJECT_POM=$1/pom.xml
  local TARGET_VERSION=$2

  xmlstarlet edit -L -u "/_:project/_:version" -v $TARGET_VERSION $PROJECT_POM

  for MODULE in $(xmlstarlet sel -t -v "/_:project/_:modules/_:module" $PROJECT_POM); do
    # update here the parent reference version
    xmlstarlet edit -L -u "/_:project/_:parent/_:version" -v $TARGET_VERSION "$PROJECT/$MODULE/pom.xml"

    updateVersions "$PROJECT/$MODULE" $TARGET_VERSION
  done;
}

function prepareFromLocal {
  if [ -d $WORK_DIR ]; then
    rm -Rf $WORK_DIR
  fi

  mkdir $WORK_DIR

  cp -R . $WORK_DIR
  cd $WORK_DIR
}

function prepareFromRemote {
  if [ -d $WORK_DIR ]; then
    rm -Rf $WORK_DIR
  fi

  git clone $GIT_REPO $WORK_DIR
  cd $WORK_DIR
}


# Install deps required to build and run native thrift
function installNativeThrift {
  local CURR_DIR=$PWD

  if [ ! -d $THRIFT7_PATH ] || [ ! -d $THRIFT9_PATH ]; then
    cd /tmp
    sudo apt-get install -qq libboost-dev libboost-test-dev libboost-program-options-dev libevent-dev automake libtool flex bison pkg-config g++ libssl-dev
    git clone https://git-wip-us.apache.org/repos/asf/thrift.git
    cd thrift

    if [ ! -d $THRIFT7_PATH ]; then
      git checkout 0.7.0
      ./bootstrap.sh
      ./configure --disable-gen-erl --disable-gen-hs --without-ruby --without-haskell --without-python --without-erlang --prefix=$THRIFT7_PATH JAVA_PREFIX=$THRIFT7_PATH/lib/

      # See https://issues.apache.org/jira/browse/THRIFT-1614 a solution would be to use two different versions of automake
      # but this would be more complex. The other option is to change the include in thriftl.cc but I don't like that much either.
      set +e
      make install > /dev/null 2>&1
      set -e
      mv compiler/cpp/thrifty.hh compiler/cpp/thrifty.h

      make install
      make clean
    fi

    if [ ! -d $THRIFT9_PATH ]; then
      git checkout 0.9.1
      ./bootstrap.sh
      ./configure --disable-gen-erl --disable-gen-hs --without-ruby --without-haskell --without-python --without-erlang --prefix=$THRIFT9_PATH JAVA_PREFIX=$THRIFT9_PATH/lib/
      make install
    fi

    cd ..
    rm -Rf thrift
  fi

  cd $CURR_DIR
}

function installProtobuf {
  local CURR_DIR=$PWD

  if [ ! -d $PROTOBUF_PATH ]; then
    cd /tmp
    wget https://github.com/google/protobuf/releases/download/v2.4.1/protobuf-2.4.1.tar.gz -O - | tar -xz
    cd protobuf-2.4.1
    ./configure --prefix=$PROTOBUF_PATH
    make install
    cd ..
    rm -Rf protobuf-2.4.1
  fi

  cd $CURR_DIR
}

# Install deps required to build hadoop lzo and native libgplcompression
function installHadoopLzo {
  local CURR_DIR=$PWD

  if [ ! -d $HADOOP_LZO_PATH ]; then

    if [ -z ${JAVA_HOME+x} ]; then
      echo "Please enter a value for JAVA_HOME:"
      read JAVA_HOME
    fi

    cd /tmp
    sudo apt-get -qq install lzop liblzo2-dev
    git clone git://github.com/twitter/hadoop-lzo.git
    cd hadoop-lzo
    mvn compile
    mv target/native/Linux-* $HADOOP_LZO_PATH
    cd ..
    rm -Rf hadoop-lzo
  fi

  cd $CURR_DIR
}

#######################################################################################################################
#######################################################################################################################
#######################################################################################################################

__MVN_THRIFT7="-Pthrift7 -Dthrift.executable=$THRIFT7_PATH/bin/thrift"
__MVN_THRIFT9="-Pthrift9 -Dthrift.executable=$THRIFT9_PATH/bin/thrift"
__MVN_HADOOP_LZO="-Dtest.library.path=$HADOOP_LZO_PATH/lib -Drequire.lzo.tests=true"
__MVN_PROTOC_EXECUTABLE="-Dprotoc.executable=$PROTOBUF_PATH/bin/protoc"

case "$COMMAND" in
"test")
    prepareFromLocal
    git checkout $BASE_BRANCH

    installNativeThrift
    installHadoopLzo
    installProtobuf

    mvn clean test $__MVN_THRIFT7 $__MVN_HADOOP_LZO $__MVN_PROTOC_EXECUTABLE
    mvn clean test $__MVN_THRIFT9 $__MVN_HADOOP_LZO $__MVN_PROTOC_EXECUTABLE
    ;;
"install")
    echo "Will install current version"
    prepareFromLocal
    git checkout $BASE_BRANCH

    installNativeThrift
    installHadoopLzo
    installProtobuf

    mvn clean install $__MVN_THRIFT7 $__MVN_HADOOP_LZO $__MVN_PROTOC_EXECUTABLE
    mvn clean install $__MVN_THRIFT9 $__MVN_HADOOP_LZO $__MVN_PROTOC_EXECUTABLE
    ;;
"deploy")
    echo "Will deploy current version"
    prepareFromLocal
    git checkout $BASE_BRANCH

    installNativeThrift
    installHadoopLzo
    installProtobuf

    mvn clean deploy $__MVN_THRIFT7 $__MVN_HADOOP_LZO $__MVN_PROTOC_EXECUTABLE
    mvn clean deploy $__MVN_THRIFT9 $__MVN_HADOOP_LZO $__MVN_PROTOC_EXECUTABLE
    ;;
"release")
    while [ -z ${NEXT_DEV_VERSION+x} ] || [[ $NEXT_DEV_VERSION != *"-SNAPSHOT" ]]; do
      echo "What is the next dev version (must be of the standard form XXX-SNAPSHOT)?"
      read NEXT_DEV_VERSION
    done

    echo "Will run full release including: release branch, deploy artifacts and update current branch to next version"

    checkNoUncommitedChanges
  
    # Ensure our copy is fresh
    git pull

    # We want to make the release from the initial branch, here we are in the working copy, not the original directory
    git checkout $BASE_BRANCH

    installNativeThrift
    installHadoopLzo
    installProtobuf

    # Update the version to use the release version, sync with scm and deploy
    updateVersions . $RELEASE_VERSION

    git add pom.xml **/pom.xml
    git commit -m "[Release] - Prepare release $RELEASE_VERSION"
    git tag "elephant-bird-$RELEASE_VERSION"

    mvn clean deploy $__MVN_THRIFT7 $__MVN_HADOOP_LZO $__MVN_PROTOC_EXECUTABLE -DperformRelease=true
    mvn clean deploy $__MVN_THRIFT9 $__MVN_HADOOP_LZO $__MVN_PROTOC_EXECUTABLE -DperformRelease=true


    # Update to the next development version and push those changes to master
    updateVersions . $NEXT_DEV_VERSION
    git add pom.xml **/pom.xml
    git commit -m "[Release] - $RELEASE_VERSION, prepare for next development iteration $NEXT_DEV_VERSION"

    # Until here we are supposed to be able to easily revert things as we still have our unchanged clone
    cd $BASE_DIR
    git pull origin $BASE_BRANCH

    echo "Local tag created named elephant-bird-$RELEASE_VERSION"
    echo "Local branch $BASE_BRANCH updated"
    echo "Use git push origin HEAD && git push --tags"
    echo "to update the remote if all looks good"
    ;;
*)
    echo "Unknown command: $COMMAND"
    exit 1;
esac

# Cleaning after us (in case of an error we want the src to remain so we can debug things)
rm -Rf /tmp/elephant-bird_release
