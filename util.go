package kubejob

import (
	corev1 "k8s.io/api/core/v1"
)

const jobCommandTemplate = `
while [ ! -f /tmp/kubejob-status ]
do
    sleep 1;
done

exit $(cat /tmp/kubejob-status)
`

func jobTemplateCommandContainer(c corev1.Container) corev1.Container {
	copied := c.DeepCopy()
	copied.Command = []string{"sh", "-c"}
	copied.Args = []string{jobCommandTemplate}
	return *copied
}
