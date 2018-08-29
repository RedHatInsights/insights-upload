pipeline {
  agent {
    node {
      label 'python3'
    }
  }
  environment {
    ASYNC_TEST_TIMEOUT=10
  }
  stages {
    stage('Testing') {
      steps {
        echo "Setting up environment..."
        sh '/bin/python36 -m pip install --user -r requirements.txt'
        echo "Testing with pytest..."
        sh '/bin/python36 -m pytest'
        echo "Testing with flake8"
        sh '/bin/python36 -m flake8'
      }
    }
  }
}
