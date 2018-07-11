pipeline {
  agent {
    node {
      label 'python3'
    }
  }
  stages {
    stage('Testing') {
      steps {
        echo "Testing"
        sh """
           virtualenv .
           source bin/activate
           pip install -r requirements.txt
           pytest
           """
      }
    }
  }
}
