rm dist/*.whl

VERSION=$1

export DEV_VERSION=$VERSION

python3.10 setup.py bdist_wheel


aws s3 cp --acl bucket-owner-full-control --force-glacier-transfer dist/prophecy_build_tool-1.3.8.dev$VERSION-py3-none-any.whl s3://prophecy-public-bucket/execution/pbt_prophecy/prophecy_build_tool-1.3.8.dev$VERSION-py3-none-any.whl

echo "Running in Pod: cd /tmp && wget https://prophecy-public-bucket.s3.us-east-2.amazonaws.com/execution/pbt_prophecy/prophecy_build_tool-1.3.8.dev$VERSION-py3-none-any.whl && pip install --force prophecy_build_tool-1.3.8.dev$VERSION-py3-none-any.whl"

# Find the execution pod name
EXEC_POD=$(kubectl get pods --context ishaan-dev-k3s --namespace=prophecy -o name | grep '^pod/execution' | head -n1 | cut -d/ -f2)

# Run the install command inside the execution pod
kubectl exec --context ishaan-dev-k3s --namespace=prophecy "$EXEC_POD" -- /bin/sh -c "cd /tmp && wget https://prophecy-public-bucket.s3.us-east-2.amazonaws.com/execution/pbt_prophecy/prophecy_build_tool-1.3.8.dev$VERSION-py3-none-any.whl && pip install --force prophecy_build_tool-1.3.8.dev$VERSION-py3-none-any.whl && export DEV_VERSION=$VERSION"