# Druid MM Autoscaler

MIDAS is an advanced autoscaler designed for Druid's MiddleManager service. It intelligently scales your MiddleManagers, ensuring optimal resource utilization while preventing unexpected ingestion failures.

How to Use?
Configure MIDAS:

1. Navigate to druid_conf.yaml and update the configurations to match your Druid Kubernetes setup.
2. Build Docker Image:
Run the following command to build the Docker image:
"docker build -t image_name:tag ."

3. Push the image to your desired container registry (Docker Hub, ECR, etc.):
Run the following command to push the Docker image:
docker push <your_registry>/<image_name>:<tag_name>

4. Update Deployment: 
Edit deployment.yaml in prod/staging according to your Druid environment.
Update the image reference in the deployment file to match the one you've built and pushed.

5. Create Role and RoleBinding:
Ensure proper permissions by creating a Role and RoleBinding for the autoscaler.

6. Deploy MIDAS to Kubernetes:
Use the following commands to deploy the autoscaler in your Kubernetes cluster:
"kubectl kustomize ./k8s/prod/kustomization"
"kubectl apply -k ./k8s/prod/kustomization"

Note: We've also built and pushed a MIDAS image to Docker Hub, which is currently being used in the deployment. If your setup matches ours, you can directly deploy MIDAS to your cluster without needing to build or push a new image.
