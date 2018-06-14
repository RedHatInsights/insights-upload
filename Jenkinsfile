pipeline {
  agent {
    node {
      label 'python'
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
           python -m tornado.test.runtests tests.test_app
           """
      }
    }
  }
}
