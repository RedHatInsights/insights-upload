def run_test(){
    echo "Running unit-test..."
    sh'''
        /bin/python36 -m pytest -rxX --capture=sys -v --junitxml=unit-tests-report.xml --html=unit-tests-report.html --self-contained-html --cov=. --cov-report html
    '''
}

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
    stage('setup') {
      steps {
        echo "Setting up environment..."
        sh '/bin/python36 -m pip install --user -r requirements.txt'
      }
    }

    stage('code-check') {
      steps {
        echo "Checking code with flake8"
        sh '/bin/python36 -m flake8'
      }
    }

    stage('unit-tests') {
      steps {
        run_test()
      }
      post {
        always {
            archiveArtifacts allowEmptyArchive: true, artifacts: "*.xml, *.html, htmlcov/*.*"
            junit 'unit-tests-report.xml'
        }
      }

    }
  }
}
