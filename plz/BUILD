sh_cmd(
    name = "start",
    cmd = [
        "plz run //plz/minikube:minikube -- start",
        "plz run //plz/minikube:minikube -- addons enable ingress",
        "plz run //plz/minikube:k8s_push",
    ],
)

#kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v0.44.0/deploy/static/provider/cloud/deploy.yaml
