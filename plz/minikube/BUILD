subinclude("///pleasings//k8s")


remote_file (
    name = "minikube",
    url = f"https://storage.googleapis.com/minikube/releases/latest/minikube-{CONFIG.OS}-amd64",
    binary = True,
)

remote_file (
    name = "kubectl",
    url = f"https://dl.k8s.io/release/v1.20.0/bin/{CONFIG.OS}/amd64/kubectl",
    binary = True,
)

k8s_config(
    name = "k8s",
    srcs = [
        "prefect.yaml",
    ],
    containers = [],
)

sh_cmd(
    name = "start",
    cmd = [
        "plz run //minikube:minikube -- start",
        "plz run //minikube:minikube -- addons enable ingress",
        "plz run //minikube:k8s_push",
    ],
)
