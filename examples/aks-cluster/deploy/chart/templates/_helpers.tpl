{{/* Resources append suffixes (-host, -redis, -loadgen-<suffix>), so cap
     the base well under the 63-char resource-name limit. */}}
{{- define "chart.fullname" -}}
{{- printf "%s" .Release.Name | trunc 40 | trimSuffix "-" -}}
{{- end -}}

{{- define "chart.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "chart.hostSelector" -}}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: host
{{- end -}}

{{- define "chart.redisSelector" -}}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: redis
{{- end -}}

{{/* Anti-affinity term against a component, on the hostname topology. */}}
{{- define "chart.antiAffinityTerm" -}}
labelSelector:
  matchLabels:
    app.kubernetes.io/instance: {{ .instance }}
    app.kubernetes.io/component: {{ .component }}
topologyKey: kubernetes.io/hostname
{{- end -}}
