import axios from 'axios'
import { useEffect, useMemo, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

const api = axios.create({ baseURL: 'http://localhost:8000' })

type View = 'dashboard' | 'appointments' | 'appointment-detail' | 'engineers' | 'engineer-detail' | 'tqr-report'
type Nullable<T> = T | null

type TqrResult = {
  appointment_id?: string
  workmanship: number
  cleanliness: number
  safety: number
  completion: number  
  overall: number
  summary: string
  flags: string[]
  recommendation: string
  analysed_at?: string
}

type Appointment = {
  Id: string
  AppointmentNumber?: string
  Status?: string
  Subject?: string
  Description?: string
  ActualEndTime?: string
  ActualEndTimeFormatted?: string
  Trade_Group_Postcode__c?: string
  Allocated_Engineer__c?: string
  Feedback_Notes__c?: string
  Scheduled_Trade__c?: string
  Attendance_Report_for_Customer__c?: string
  Workmanship__c?: number
  CCT_Charge_Gross__c?: number
  SchedStartTime?: string
  SchedStartTimeFormatted?: string
  ArrivalWindowStartTime?: string
  ArrivalWindowStartTimeFormatted?: string
  Duration?: number
  Street?: string
  City?: string
  PostalCode?: string
  AccountId?: string
  tqrScore?: number | null
  tqrResult?: Nullable<TqrResult>
  imageMetadata?: Array<{ id: string; title: string; contentType: string }>
  workOrderId?: string
  workOrder?: {
    Id: string
    WorkOrderNumber?: string
    Description?: string
    Street?: string
    City?: string
    PostalCode?: string
    WorkType?: { Name: string }
  }
  account?: { Id: string; Name: string; Phone?: string }
  site?: string
  accountManager?: string
}

type DashboardStats = {
  totalRecords: number
  totalCCTCharge: number
  avgTQRScore: number | null
  byTradeGroup: Array<{ name: string; count: number }>
  byStatus: Array<{ name: string; count: number }>
  topEngineers: Array<{ name: string; avgTQRScore: number; jobs: number }>
  lowScoreJobs: Array<{ id: string; appointmentNumber: string; engineer: string; trade: string; status: string; overall: number; actualEndTime: string }>
}

type EngineerSummary = {
  name: string
  totalJobs: number
  completedJobs: number
  avgTQRScore: number | null
  totalCharge: number
  lastJobDate: string | null
}

type AppointmentImage = {
  id: string
  title: string
  base64: string
  contentType: string
}

const cardClass = 'rounded-2xl border border-slate-800 bg-slate-900/80 p-5 shadow-[0_20px_80px_-32px_rgba(15,23,42,0.95)] backdrop-blur'
const sectionTitleClass = 'text-lg font-semibold tracking-wide text-slate-100'

const formatDateTime = (value?: string | null) => value || 'N/A'
const formatMoney = (value?: number | null) =>
  new Intl.NumberFormat('en-GB', { style: 'currency', currency: 'GBP' }).format(value ?? 0)

const scoreTone = (score?: number | null) => {
  if (score === null || score === undefined) return 'bg-slate-700/70 text-slate-200 border-slate-600'
  if (score >= 8) return 'bg-emerald-500/15 text-emerald-300 border-emerald-400/40'
  if (score >= 5) return 'bg-amber-500/15 text-amber-200 border-amber-400/40'
  return 'bg-rose-500/15 text-rose-200 border-rose-400/40'
}

const statusTone = (status?: string) => {
  switch (status) {
    case 'Visit Complete':
      return 'bg-emerald-500/15 text-emerald-300 border-emerald-400/40'
    case 'Scheduled':
      return 'bg-sky-500/15 text-sky-200 border-sky-400/40'
    case 'Cancelled':
      return 'bg-rose-500/15 text-rose-200 border-rose-400/40'
    case 'In Progress':
      return 'bg-amber-500/15 text-amber-200 border-amber-400/40'
    default:
      return 'bg-slate-700/70 text-slate-200 border-slate-600'
  }
}

const chartPalette = ['#38bdf8', '#2dd4bf', '#f59e0b', '#f43f5e', '#8b5cf6', '#22c55e', '#e879f9']

const StatCard = ({ label, value, accent }: { label: string; value: string; accent: string }) => (
  <div className={`${cardClass} relative overflow-hidden`}>
    <div className={`absolute inset-x-0 top-0 h-1 ${accent}`} />
    <div className="text-sm uppercase tracking-[0.3em] text-slate-400">{label}</div>
    <div className="mt-4 text-3xl font-semibold text-white">{value}</div>
  </div>
)

const StatusBadge = ({ status }: { status?: string }) => (
  <span className={`inline-flex rounded-full border px-3 py-1 text-xs font-semibold ${statusTone(status)}`}>{status || 'Unknown'}</span>
)

const TQRScoreBadge = ({ score }: { score?: number | null }) => (
  <span className={`inline-flex rounded-full border px-3 py-1 text-xs font-semibold ${scoreTone(score)}`}>
    {score === null || score === undefined ? 'UNSCORED' : `${score.toFixed(1)} ${score >= 8 ? 'PASS' : score >= 5 ? 'REVIEW' : 'FAIL'}`}
  </span>
)

const TQRScoreGauge = ({ label, score }: { label: string; score?: number | null }) => {
  const percent = Math.max(0, Math.min(((score ?? 0) / 10) * 100, 100))
  return (
    <div className={`${cardClass} p-4`}>
      <div className="mb-2 text-sm text-slate-300">{label}</div>
      <div className="h-3 overflow-hidden rounded-full bg-slate-800">
        <div className={`h-full rounded-full ${scoreTone(score).split(' ')[0]}`} style={{ width: `${percent}%` }} />
      </div>
      <div className="mt-3 text-xl font-semibold text-white">{score === null || score === undefined ? 'N/A' : score.toFixed(1)}</div>
    </div>
  )
}

const ImageGallery = ({ images }: { images: AppointmentImage[] }) => (
  <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
    {images.map((image) => (
      <div key={image.id} className={`${cardClass} p-3`}>
        <img className="h-52 w-full rounded-xl object-cover" src={`data:${image.contentType};base64,${image.base64}`} alt={image.title} />
        <div className="mt-3 text-sm text-slate-300">{image.title}</div>
      </div>
    ))}
  </div>
)

const DetailItem = ({ label, value }: { label: string; value?: string | number | null }) => (
  <div className="rounded-xl border border-slate-800 bg-slate-950/70 p-4">
    <div className="text-xs uppercase tracking-[0.25em] text-slate-500">{label}</div>
    <div className="mt-2 text-sm text-slate-100">{value || 'N/A'}</div>
  </div>
)

const ModalField = ({ label, value }: { label: string; value?: string | null }) => (
  <div className="flex flex-col border-b border-slate-800 py-3 last:border-0">
    <div className="text-xs uppercase tracking-[0.2em] text-slate-500">{label}</div>
    <div className="mt-1 text-sm font-medium text-cyan-300">{value || '—'}</div>
  </div>
)

const AppointmentModal = ({ id, onClose }: { id: string; onClose: () => void }) => {
  const [detail, setDetail] = useState<Appointment | null>(null)
  const [images, setImages] = useState<AppointmentImage[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    setDetail(null)
    setImages([])
    const load = async () => {
      try {
        const { data } = await api.get<Appointment>(`/api/appointments/${id}`)
        setDetail(data)
        const woId = data.workOrderId
        if (woId) {
          const { data: imgData } = await api.get<{ images: AppointmentImage[] }>(`/api/work-orders/${woId}/images`)
          setImages(imgData.images ?? [])
        }
      } finally {
        setLoading(false)
      }
    }
    void load()
  }, [id])

  const workOrderAddress = detail?.workOrder
    ? [detail.workOrder.Street, detail.workOrder.City, detail.workOrder.PostalCode].filter(Boolean).join(', ')
    : null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm" onClick={onClose}>
      <div
        className="relative max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-2xl border border-slate-700 bg-slate-900 p-6 shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          className="absolute right-4 top-4 rounded-lg border border-slate-700 px-2 py-1 text-xs text-slate-400 hover:border-slate-500 hover:text-white"
          onClick={onClose}
        >
          ✕ Close
        </button>

        <div className="mb-5 pr-16">
          <div className="text-xs uppercase tracking-[0.35em] text-slate-500">Appointment Details</div>
          <div className="mt-2 text-2xl font-semibold text-white">{detail?.AppointmentNumber || id}</div>
          {detail && <StatusBadge status={detail.Status} />}
        </div>

        {loading ? (
          <div className="py-10 text-center text-slate-400">Loading details...</div>
        ) : detail ? (
          <div className="space-y-6">
            <div className="rounded-xl border border-slate-800 bg-slate-950/60 px-4">
              <ModalField label="Account" value={detail.account?.Name} />
              <ModalField label="Account Manager" value={detail.accountManager} />
              <ModalField
                label="Site"
                value={detail.site || workOrderAddress || [detail.Street, detail.City, detail.PostalCode].filter(Boolean).join(', ')}
              />
              <ModalField label="Work Order" value={detail.workOrder?.WorkOrderNumber} />
              <ModalField label="Work Type" value={detail.workOrder?.WorkType?.Name || detail.Scheduled_Trade__c} />
              <ModalField label="Description" value={detail.Description} />
              <ModalField label="Engineer" value={detail.Allocated_Engineer__c} />
              <ModalField label="Actual End" value={detail.ActualEndTimeFormatted} />
            </div>

            {images.length > 0 && (
              <div>
                <div className="mb-3 text-sm font-semibold text-slate-200">
                  Notes &amp; Attachments ({images.length} items)
                </div>
                <div className="grid gap-3 sm:grid-cols-2">
                  {images.map((img) => (
                    <div key={img.id} className="rounded-xl border border-slate-800 bg-slate-950/60 p-2">
                      <img
                        className="h-44 w-full rounded-lg object-cover"
                        src={`data:${img.contentType};base64,${img.base64}`}
                        alt={img.title}
                      />
                      <div className="mt-2 truncate px-1 text-xs text-slate-400">{img.title}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {images.length === 0 && !loading && (
              <div className="rounded-xl border border-slate-800 bg-slate-950/60 py-6 text-center text-sm text-slate-500">
                No attachments found for this work order.
              </div>
            )}
          </div>
        ) : (
          <div className="py-10 text-center text-slate-400">Could not load appointment details.</div>
        )}
      </div>
    </div>
  )
}

const useAppointments = () => {
  const [appointments, setAppointments] = useState<Appointment[]>([])
  const [loading, setLoading] = useState(false)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(15)
  const [totalPages, setTotalPages] = useState(1)
  const [total, setTotal] = useState(0)
  const [filters, setFilters] = useState({ search: '', status: '', engineer: '', trade: '' })

  const fetchAppointments = async (nextPage = page, nextPageSize = pageSize) => {
    setLoading(true)
    try {
      const { data } = await api.get('/api/appointments', {
        params: { page: nextPage, pageSize: nextPageSize, ...filters },
      })
      setAppointments(data.records)
      setPage(data.page)
      setPageSize(data.pageSize)
      setTotalPages(data.totalPages)
      setTotal(data.total)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void fetchAppointments(1, pageSize)
  }, [filters.search, filters.status, filters.engineer, filters.trade])

  return {
    appointments,
    loading,
    page,
    pageSize,
    totalPages,
    total,
    filters,
    setFilters,
    fetchAppointments,
    setPage,
    setPageSize,
  }
}

const DashboardView = ({
  stats,
  recentAppointments,
  onOpenAppointment,
  onOpenModal,
}: {
  stats: DashboardStats | null
  recentAppointments: Appointment[]
  onOpenAppointment: (id: string) => void
  onOpenModal: (id: string) => void
}) => (
  <div className="space-y-8">
    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
      <StatCard label="Total Records" value={`${stats?.totalRecords ?? 0}`} accent="bg-sky-400" />
      <StatCard label="Total CCT Charge £" value={formatMoney(stats?.totalCCTCharge)} accent="bg-teal-400" />
      <StatCard label="Avg TQR Score" value={stats?.avgTQRScore?.toFixed(1) ?? 'N/A'} accent="bg-amber-400" />
      <StatCard label="Flagged Jobs" value={`${stats?.lowScoreJobs.length ?? 0}`} accent="bg-rose-400" />
    </div>

    <div className="grid gap-6 xl:grid-cols-2">
      <div className={cardClass}>
        <div className={sectionTitleClass}>Record Count by Trade Group Postcode</div>
        <div className="mt-6 h-80">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={stats?.byTradeGroup ?? []}>
              <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" />
              <XAxis dataKey="name" stroke="#94a3b8" />
              <YAxis stroke="#94a3b8" />
              <Tooltip />
              <Bar dataKey="count" radius={[8, 8, 0, 0]} fill="#38bdf8" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
      <div className={cardClass}>
        <div className={sectionTitleClass}>Jobs by Status</div>
        <div className="mt-6 h-80">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={stats?.byStatus ?? []}>
              <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" />
              <XAxis dataKey="name" stroke="#94a3b8" />
              <YAxis stroke="#94a3b8" />
              <Tooltip />
              <Bar dataKey="count" radius={[8, 8, 0, 0]}>
                {(stats?.byStatus ?? []).map((entry, index) => (
                  <Cell key={entry.name} fill={chartPalette[index % chartPalette.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>

    <div className={cardClass}>
      <div className="mb-4 flex items-center justify-between">
        <div className={sectionTitleClass}>10 Most Recent Jobs</div>
        <div className="text-sm text-slate-400">Click appt number to preview · Click row for full detail</div>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-left text-sm">
          <thead className="text-slate-400">
            <tr>
              <th className="pb-3">Trade Group</th>
              <th className="pb-3">Engineer</th>
              <th className="pb-3">Appt No</th>
              <th className="pb-3">Status</th>
              <th className="pb-3">Trade</th>
              <th className="pb-3">Actual End</th>
              <th className="pb-3">Charge</th>
              <th className="pb-3">TQR Score</th>
            </tr>
          </thead>
          <tbody>
            {recentAppointments.slice(0, 10).map((appt) => (
              <tr key={appt.Id} className="cursor-pointer border-t border-slate-800 text-slate-200 hover:bg-slate-800/50" onClick={() => onOpenAppointment(appt.Id)}>
                <td className="py-3">{appt.Trade_Group_Postcode__c || 'N/A'}</td>
                <td className="py-3">{appt.Allocated_Engineer__c || 'Unassigned'}</td>
                <td className="py-3">
                  <button
                    className="font-semibold text-cyan-400 underline underline-offset-2 hover:text-cyan-200"
                    onClick={(e) => { e.stopPropagation(); onOpenModal(appt.Id) }}
                  >
                    {appt.AppointmentNumber || 'N/A'}
                  </button>
                </td>
                <td className="py-3"><StatusBadge status={appt.Status} /></td>
                <td className="py-3">{appt.Scheduled_Trade__c || 'N/A'}</td>
                <td className="py-3">{formatDateTime(appt.ActualEndTimeFormatted)}</td>
                <td className="py-3">{formatMoney(appt.CCT_Charge_Gross__c)}</td>
                <td className="py-3"><TQRScoreBadge score={appt.tqrScore} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  </div>
)

const AppointmentsView = ({
  appointmentsState,
  onOpenAppointment,
  onOpenModal,
  onAnalyse,
  analysingIds,
}: {
  appointmentsState: ReturnType<typeof useAppointments>
  onOpenAppointment: (id: string) => void
  onOpenModal: (id: string) => void
  onAnalyse: (id: string) => Promise<void>
  analysingIds: string[]
}) => {
  const { appointments, loading, page, pageSize, totalPages, total, filters, setFilters, fetchAppointments } = appointmentsState

  const engineers = Array.from(new Set(appointments.map((item) => item.Allocated_Engineer__c).filter(Boolean))) as string[]
  const tradeGroups = Array.from(new Set(appointments.flatMap((item) => [item.Scheduled_Trade__c, item.Trade_Group_Postcode__c]).filter(Boolean))) as string[]
  const statuses = ['Visit Complete', 'Scheduled', 'Cancelled', 'In Progress']

  return (
    <div className="space-y-6">
      <div className={`${cardClass} grid gap-4 xl:grid-cols-4`}>
        <input className="rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100 outline-none" placeholder="Search appointments, engineer or trade" value={filters.search} onChange={(e) => setFilters((prev) => ({ ...prev, search: e.target.value }))} />
        <select className="rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100 outline-none" value={filters.status} onChange={(e) => setFilters((prev) => ({ ...prev, status: e.target.value }))}>
          <option value="">All Statuses</option>
          {statuses.map((status) => <option key={status} value={status}>{status}</option>)}
        </select>
        <select className="rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100 outline-none" value={filters.engineer} onChange={(e) => setFilters((prev) => ({ ...prev, engineer: e.target.value }))}>
          <option value="">All Engineers</option>
          {engineers.map((engineer) => <option key={engineer} value={engineer}>{engineer}</option>)}
        </select>
        <select className="rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100 outline-none" value={filters.trade} onChange={(e) => setFilters((prev) => ({ ...prev, trade: e.target.value }))}>
          <option value="">All Trade Groups</option>
          {tradeGroups.map((trade) => <option key={trade} value={trade}>{trade}</option>)}
        </select>
      </div>

      <div className={cardClass}>
        <div className="mb-4 flex items-center justify-between">
          <div className={sectionTitleClass}>All Appointments</div>
          <div className="text-sm text-slate-400">{loading ? 'Loading...' : `${total} records`}</div>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-left text-sm">
            <thead className="text-slate-400">
              <tr>
                <th className="pb-3">Appt No</th>
                <th className="pb-3">Engineer</th>
                <th className="pb-3">Status</th>
                <th className="pb-3">Trade</th>
                <th className="pb-3">Actual End</th>
                <th className="pb-3">Charge £</th>
                <th className="pb-3">TQR Score</th>
                <th className="pb-3">Analyse</th>
              </tr>
            </thead>
            <tbody>
              {appointments.map((appt) => (
                <tr key={appt.Id} className="border-t border-slate-800 text-slate-200 hover:bg-slate-800/50">
                  <td className="py-3">
                    <button
                      className="font-semibold text-cyan-400 underline underline-offset-2 hover:text-cyan-200"
                      onClick={() => onOpenModal(appt.Id)}
                    >
                      {appt.AppointmentNumber || 'N/A'}
                    </button>
                  </td>
                  <td className="py-3 cursor-pointer" onClick={() => onOpenAppointment(appt.Id)}>{appt.Allocated_Engineer__c || 'Unassigned'}</td>
                  <td className="py-3 cursor-pointer" onClick={() => onOpenAppointment(appt.Id)}><StatusBadge status={appt.Status} /></td>
                  <td className="py-3 cursor-pointer" onClick={() => onOpenAppointment(appt.Id)}>{appt.Scheduled_Trade__c || appt.Trade_Group_Postcode__c || 'N/A'}</td>
                  <td className="py-3 cursor-pointer" onClick={() => onOpenAppointment(appt.Id)}>{formatDateTime(appt.ActualEndTimeFormatted)}</td>
                  <td className="py-3 cursor-pointer" onClick={() => onOpenAppointment(appt.Id)}>{formatMoney(appt.CCT_Charge_Gross__c)}</td>
                  <td className="py-3 cursor-pointer" onClick={() => onOpenAppointment(appt.Id)}><TQRScoreBadge score={appt.tqrScore} /></td>
                  <td className="py-3">
                    <button
                      className="rounded-xl border border-cyan-400/30 bg-cyan-400/10 px-4 py-2 text-xs font-semibold text-cyan-200 disabled:cursor-not-allowed disabled:opacity-40"
                      disabled={Boolean(appt.tqrResult) || analysingIds.includes(appt.Id)}
                      onClick={() => void onAnalyse(appt.Id)}
                    >
                      {appt.tqrResult ? 'Cached' : analysingIds.includes(appt.Id) ? 'Analysing...' : 'Analyse'}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="mt-5 flex items-center justify-between text-sm text-slate-400">
          <button className="rounded-lg border border-slate-700 px-3 py-2 disabled:opacity-40" disabled={page <= 1} onClick={() => void fetchAppointments(page - 1, pageSize)}>Previous</button>
          <div>Page {page} of {totalPages}</div>
          <button className="rounded-lg border border-slate-700 px-3 py-2 disabled:opacity-40" disabled={page >= totalPages} onClick={() => void fetchAppointments(page + 1, pageSize)}>Next</button>
        </div>
      </div>
    </div>
  )
}

const AppointmentDetailView = ({
  id,
  onBack,
  onOpenEngineer,
}: {
  id: string
  onBack: () => void
  onOpenEngineer: (name: string) => void
}) => {
  const [appointment, setAppointment] = useState<Appointment | null>(null)
  const [images, setImages] = useState<AppointmentImage[]>([])
  const [loading, setLoading] = useState(true)
  const [analysing, setAnalysing] = useState(false)
  const [analysis, setAnalysis] = useState<TqrResult | null>(null)

  const loadDetail = async () => {
    setLoading(true)
    try {
      const [{ data: appointmentData }, { data: imageData }] = await Promise.all([
        api.get(`/api/appointments/${id}`),
        api.get(`/api/appointments/${id}/images`),
      ])
      setAppointment(appointmentData)
      setAnalysis(appointmentData.tqrResult ?? null)
      setImages(imageData.images ?? [])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void loadDetail()
  }, [id])

  const runAnalysis = async () => {
    setAnalysing(true)
    try {
      const { data } = await api.post(`/api/analyse/${id}`)
      setAnalysis(data)
    } finally {
      setAnalysing(false)
    }
  }

  if (loading || !appointment) {
    return <div className={cardClass}>Loading appointment...</div>
  }

  return (
    <div className="space-y-6">
      <button className="rounded-xl border border-slate-700 px-4 py-2 text-sm text-slate-200" onClick={onBack}>Back</button>

      <div className={`${cardClass} grid gap-4 md:grid-cols-3`}>
        <DetailItem label="Booking Details" value={`${appointment.AppointmentNumber || 'N/A'} | ${appointment.Status || 'Unknown'} | ${appointment.Subject || 'N/A'}`} />
        <DetailItem label="Arrival & Timing" value={`${formatDateTime(appointment.SchedStartTimeFormatted)} | ${formatDateTime(appointment.ArrivalWindowStartTimeFormatted)} | ${formatDateTime(appointment.ActualEndTimeFormatted)} | ${appointment.Duration || 0} mins`} />
        <DetailItem label="Address & Customer" value={`${appointment.Street || ''}, ${appointment.City || ''}, ${appointment.PostalCode || ''} | ${appointment.AccountId || 'N/A'}`} />
        <DetailItem label="Scope of Works" value={appointment.Description} />
        <DetailItem label="Allocated Engineer" value={appointment.Allocated_Engineer__c || 'Unassigned'} />
        <DetailItem label="Attendance Report" value={appointment.Attendance_Report_for_Customer__c} />
        <DetailItem label="Feedback Notes" value={appointment.Feedback_Notes__c} />
        <DetailItem label="Charges & Invoice" value={formatMoney(appointment.CCT_Charge_Gross__c)} />
        <div className="rounded-xl border border-slate-800 bg-slate-950/70 p-4">
          <div className="text-xs uppercase tracking-[0.25em] text-slate-500">Engineer Drilldown</div>
          <button className="mt-3 rounded-xl border border-cyan-400/30 bg-cyan-400/10 px-4 py-2 text-sm text-cyan-200" onClick={() => appointment.Allocated_Engineer__c && onOpenEngineer(appointment.Allocated_Engineer__c)}>
            Open Engineer Detail
          </button>
        </div>
      </div>

      <div className={cardClass}>
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <div className={sectionTitleClass}>TQR Check - AI Analysis</div>
            <div className="mt-2 text-sm text-slate-400">{images.length} site images available</div>
          </div>
          <button
            className="rounded-xl border border-emerald-400/30 bg-emerald-400/10 px-4 py-3 text-sm font-semibold text-emerald-200 disabled:cursor-not-allowed disabled:opacity-40"
            onClick={() => void runAnalysis()}
            disabled={analysing || Boolean(analysis)}
          >
            {analysis ? 'Already Analysed' : analysing ? 'Analysing...' : 'Run TQR Analysis'}
          </button>
        </div>

        <div className="mt-6">
          <ImageGallery images={images} />
        </div>

        {analysing && <div className="mt-6 text-sm text-cyan-200">Running TQR image analysis...</div>}

        {analysis && (
          <div className="mt-8 space-y-6">
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
              <TQRScoreGauge label="Workmanship" score={analysis.workmanship} />
              <TQRScoreGauge label="Cleanliness" score={analysis.cleanliness} />
              <TQRScoreGauge label="Safety" score={analysis.safety} />
              <TQRScoreGauge label="Completion" score={analysis.completion} />
            </div>
            <div className="flex flex-wrap items-center gap-4">
              <TQRScoreBadge score={analysis.overall} />
              <div className="text-sm text-slate-400">Analysed at: {analysis.analysed_at || 'N/A'}</div>
            </div>
            <div className={`${cardClass} bg-slate-950/70`}>
              <div className="text-sm font-semibold text-white">Summary</div>
              <div className="mt-2 text-slate-300">{analysis.summary || 'No summary returned.'}</div>
            </div>
            <div className="flex flex-wrap gap-3">
              {(analysis.flags || []).map((flag) => (
                <span key={flag} className="rounded-full border border-rose-400/40 bg-rose-500/15 px-3 py-1 text-xs font-semibold text-rose-200">{flag}</span>
              ))}
            </div>
            <div className={`${cardClass} bg-slate-950/70`}>
              <div className="text-sm font-semibold text-white">Recommendation</div>
              <div className="mt-2 text-slate-300">{analysis.recommendation || 'No recommendation returned.'}</div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

const EngineersView = ({
  engineers,
  onOpenEngineer,
}: {
  engineers: EngineerSummary[]
  onOpenEngineer: (name: string) => void
}) => (
  <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
    {engineers.map((engineer) => {
      const score = engineer.avgTQRScore
      const border =
        score === null ? 'border-slate-700' : score >= 8 ? 'border-emerald-400/50' : score >= 5 ? 'border-amber-400/50' : 'border-rose-400/50'
      const passRate = engineer.totalJobs ? `${Math.round((engineer.completedJobs / engineer.totalJobs) * 100)}%` : '0%'
      return (
        <button key={engineer.name} className={`${cardClass} ${border} text-left`} onClick={() => onOpenEngineer(engineer.name)}>
          <div className="text-xl font-semibold text-white">{engineer.name}</div>
          <div className="mt-4 grid gap-3 text-sm text-slate-300">
            <div>Total Jobs: {engineer.totalJobs}</div>
            <div>Avg TQR Score: {score?.toFixed(1) ?? 'N/A'}</div>
            <div>Total Revenue: {formatMoney(engineer.totalCharge)}</div>
            <div>Pass Rate: {passRate}</div>
          </div>
        </button>
      )
    })}
  </div>
)

const EngineerDetailView = ({
  name,
  appointments,
  onBack,
  onOpenAppointment,
}: {
  name: string
  appointments: Appointment[]
  onBack: () => void
  onOpenAppointment: (id: string) => void
}) => {
  const engineerAppointments = appointments.filter((item) => item.Allocated_Engineer__c === name)
  const chartData = engineerAppointments
    .filter((item) => item.tqrScore !== null && item.tqrScore !== undefined)
    .map((item) => ({
      date: item.ActualEndTimeFormatted || item.AppointmentNumber || 'Job',
      score: item.tqrScore,
    }))
    .reverse()

  const totalRevenue = engineerAppointments.reduce((sum, item) => sum + (item.CCT_Charge_Gross__c || 0), 0)
  const average = chartData.length ? chartData.reduce((sum, item) => sum + (item.score || 0), 0) / chartData.length : null

  return (
    <div className="space-y-6">
      <button className="rounded-xl border border-slate-700 px-4 py-2 text-sm text-slate-200" onClick={onBack}>Back</button>
      <div className="grid gap-4 md:grid-cols-4">
        <StatCard label="Engineer" value={name} accent="bg-cyan-400" />
        <StatCard label="Total Jobs" value={`${engineerAppointments.length}`} accent="bg-teal-400" />
        <StatCard label="Avg TQR" value={average?.toFixed(1) ?? 'N/A'} accent="bg-amber-400" />
        <StatCard label="Revenue" value={formatMoney(totalRevenue)} accent="bg-emerald-400" />
      </div>
      <div className={cardClass}>
        <div className={sectionTitleClass}>TQR Score Trend Over Time</div>
        <div className="mt-6 h-80">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData}>
              <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" />
              <XAxis dataKey="date" stroke="#94a3b8" />
              <YAxis domain={[0, 10]} stroke="#94a3b8" />
              <Tooltip />
              <Line type="monotone" dataKey="score" stroke="#22c55e" strokeWidth={3} dot={{ r: 4 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
      <div className={cardClass}>
        <div className={sectionTitleClass}>Appointments</div>
        <div className="mt-4 overflow-x-auto">
          <table className="min-w-full text-left text-sm">
            <thead className="text-slate-400">
              <tr>
                <th className="pb-3">Appt No</th>
                <th className="pb-3">Status</th>
                <th className="pb-3">Trade</th>
                <th className="pb-3">Actual End</th>
                <th className="pb-3">Charge</th>
                <th className="pb-3">Score</th>
              </tr>
            </thead>
            <tbody>
              {engineerAppointments.map((appt) => (
                <tr key={appt.Id} className={`cursor-pointer border-t border-slate-800 ${appt.tqrScore !== null && appt.tqrScore !== undefined && appt.tqrScore < 5 ? 'bg-rose-950/30' : ''}`} onClick={() => onOpenAppointment(appt.Id)}>
                  <td className="py-3 text-slate-200">{appt.AppointmentNumber || 'N/A'}</td>
                  <td className="py-3"><StatusBadge status={appt.Status} /></td>
                  <td className="py-3 text-slate-200">{appt.Scheduled_Trade__c || appt.Trade_Group_Postcode__c || 'N/A'}</td>
                  <td className="py-3 text-slate-200">{formatDateTime(appt.ActualEndTimeFormatted)}</td>
                  <td className="py-3 text-slate-200">{formatMoney(appt.CCT_Charge_Gross__c)}</td>
                  <td className="py-3"><TQRScoreBadge score={appt.tqrScore} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

const TQRReportView = ({
  appointments,
  onAnalyse,
  analysingIds,
}: {
  appointments: Appointment[]
  onAnalyse: (id: string) => Promise<void>
  analysingIds: string[]
}) => {
  const [engineerFilter, setEngineerFilter] = useState('')
  const [tradeFilter, setTradeFilter] = useState('')
  const [scoreRange, setScoreRange] = useState('')
  const [batchProgress, setBatchProgress] = useState(0)

  const analysedJobs = useMemo(() => appointments.filter((item) => item.tqrResult), [appointments])
  const filtered = analysedJobs.filter((item) => {
    const score = item.tqrScore ?? -1
    const scoreMatch =
      !scoreRange ||
      (scoreRange === 'high' && score >= 8) ||
      (scoreRange === 'mid' && score >= 5 && score < 8) ||
      (scoreRange === 'low' && score < 5)
    const engineerMatch = !engineerFilter || item.Allocated_Engineer__c === engineerFilter
    const tradeMatch = !tradeFilter || item.Trade_Group_Postcode__c === tradeFilter || item.Scheduled_Trade__c === tradeFilter
    return scoreMatch && engineerMatch && tradeMatch
  })

  const exportCsv = () => {
    const rows = [
      ['Appointment', 'Engineer', 'Trade Group', 'Workmanship', 'Cleanliness', 'Safety', 'Completion', 'Overall'],
      ...filtered.map((item) => [
        item.AppointmentNumber || '',
        item.Allocated_Engineer__c || '',
        item.Trade_Group_Postcode__c || item.Scheduled_Trade__c || '',
        item.tqrResult?.workmanship?.toString() || '',
        item.tqrResult?.cleanliness?.toString() || '',
        item.tqrResult?.safety?.toString() || '',
        item.tqrResult?.completion?.toString() || '',
        item.tqrResult?.overall?.toString() || '',
      ]),
    ]
    const csv = rows.map((row) => row.map((cell) => `"${String(cell).replace(/"/g, '""')}"`).join(',')).join('\n')
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = 'chumley-tqr-report.csv'
    link.click()
    URL.revokeObjectURL(url)
  }

  const analyseAllUnscored = async () => {
    const targets = appointments.filter((item) => !item.tqrResult)
    let completed = 0
    for (const item of targets) {
      await onAnalyse(item.Id)
      completed += 1
      setBatchProgress(Math.round((completed / targets.length) * 100))
    }
  }

  const engineerOptions = Array.from(new Set(appointments.map((item) => item.Allocated_Engineer__c).filter(Boolean))) as string[]
  const tradeOptions = Array.from(new Set(appointments.flatMap((item) => [item.Trade_Group_Postcode__c, item.Scheduled_Trade__c]).filter(Boolean))) as string[]

  return (
    <div className="space-y-6">
      <div className={`${cardClass} grid gap-4 xl:grid-cols-5`}>
        <select className="rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100" value={scoreRange} onChange={(e) => setScoreRange(e.target.value)}>
          <option value="">All Score Ranges</option>
          <option value="high">8-10 PASS</option>
          <option value="mid">5-7 REVIEW</option>
          <option value="low">Below 5 FAIL</option>
        </select>
        <select className="rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100" value={engineerFilter} onChange={(e) => setEngineerFilter(e.target.value)}>
          <option value="">All Engineers</option>
          {engineerOptions.map((item) => <option key={item} value={item}>{item}</option>)}
        </select>
        <select className="rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100" value={tradeFilter} onChange={(e) => setTradeFilter(e.target.value)}>
          <option value="">All Trade Groups</option>
          {tradeOptions.map((item) => <option key={item} value={item}>{item}</option>)}
        </select>
        <button className="rounded-xl border border-cyan-400/30 bg-cyan-400/10 px-4 py-3 text-sm font-semibold text-cyan-200" onClick={exportCsv}>Export CSV</button>
        <button className="rounded-xl border border-emerald-400/30 bg-emerald-400/10 px-4 py-3 text-sm font-semibold text-emerald-200" onClick={() => void analyseAllUnscored()}>
          Analyse All Unscored
        </button>
      </div>

      <div className={cardClass}>
        <div className="mb-4 flex items-center justify-between">
          <div className={sectionTitleClass}>Analysed Jobs</div>
          <div className="text-sm text-slate-400">{analysingIds.length ? `Active analyses: ${analysingIds.length}` : `Batch progress: ${batchProgress}%`}</div>
        </div>
        <div className="mb-4 h-3 overflow-hidden rounded-full bg-slate-800">
          <div className="h-full rounded-full bg-emerald-400" style={{ width: `${batchProgress}%` }} />
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-left text-sm">
            <thead className="text-slate-400">
              <tr>
                <th className="pb-3">Appt No</th>
                <th className="pb-3">Engineer</th>
                <th className="pb-3">Trade Group</th>
                <th className="pb-3">Workmanship</th>
                <th className="pb-3">Cleanliness</th>
                <th className="pb-3">Safety</th>
                <th className="pb-3">Completion</th>
                <th className="pb-3">Overall</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((item) => (
                <tr key={item.Id} className="border-t border-slate-800 text-slate-200">
                  <td className="py-3">{item.AppointmentNumber || 'N/A'}</td>
                  <td className="py-3">{item.Allocated_Engineer__c || 'Unassigned'}</td>
                  <td className="py-3">{item.Trade_Group_Postcode__c || item.Scheduled_Trade__c || 'N/A'}</td>
                  <td className="py-3"><TQRScoreBadge score={item.tqrResult?.workmanship} /></td>
                  <td className="py-3"><TQRScoreBadge score={item.tqrResult?.cleanliness} /></td>
                  <td className="py-3"><TQRScoreBadge score={item.tqrResult?.safety} /></td>
                  <td className="py-3"><TQRScoreBadge score={item.tqrResult?.completion} /></td>
                  <td className="py-3"><TQRScoreBadge score={item.tqrResult?.overall} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

export default function Dashboard() {
  const [view, setView] = useState<View>('dashboard')
  const [selectedId, setSelectedId] = useState<Nullable<string>>(null)
  const [selectedEngineer, setSelectedEngineer] = useState<Nullable<string>>(null)
  const [stats, setStats] = useState<DashboardStats | null>(null)
  const [engineers, setEngineers] = useState<EngineerSummary[]>([])
  const [recentAppointments, setRecentAppointments] = useState<Appointment[]>([])
  const [allAppointments, setAllAppointments] = useState<Appointment[]>([])
  const [analysingIds, setAnalysingIds] = useState<string[]>([])
  const [modalId, setModalId] = useState<Nullable<string>>(null)
  const appointmentsState = useAppointments()

  const loadDashboard = async () => {
    const [{ data: statsData }, { data: appointmentData }, { data: engineersData }] = await Promise.all([
      api.get('/api/dashboard/stats'),
      api.get('/api/appointments', { params: { page: 1, pageSize: 100 } }),
      api.get('/api/engineers'),
    ])
    setStats(statsData)
    setRecentAppointments(appointmentData.records ?? [])
    setAllAppointments(appointmentData.records ?? [])
    setEngineers(engineersData ?? [])
  }

  useEffect(() => {
    void loadDashboard()
  }, [])

  useEffect(() => {
    setAllAppointments(appointmentsState.appointments)
  }, [appointmentsState.appointments])

  const openAppointment = (id: string) => {
    setSelectedId(id)
    setView('appointment-detail')
  }

  const openEngineer = (name: string) => {
    setSelectedEngineer(name)
    setView('engineer-detail')
  }

  const runAnalysis = async (id: string) => {
    setAnalysingIds((prev) => [...prev, id])
    try {
      await api.post(`/api/analyse/${id}`)
      await Promise.all([appointmentsState.fetchAppointments(), loadDashboard()])
    } finally {
      setAnalysingIds((prev) => prev.filter((item) => item !== id))
    }
  }

  const navItems: Array<{ key: View; label: string }> = [
    { key: 'dashboard', label: '🏠 Dashboard' },
    { key: 'appointments', label: '📋 Appointments' },
    { key: 'engineers', label: '👷 Engineers' },
    { key: 'tqr-report', label: '✅ TQR Report' },
  ]

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top,_rgba(14,165,233,0.2),_transparent_35%),linear-gradient(180deg,_#020617_0%,_#0f172a_35%,_#020617_100%)] text-slate-100">
      {modalId && <AppointmentModal id={modalId} onClose={() => setModalId(null)} />}
      <div className="mx-auto flex min-h-screen max-w-[1800px] flex-col lg:flex-row">
        <aside className="border-b border-slate-800 bg-slate-950/90 px-6 py-8 lg:min-h-screen lg:w-72 lg:border-b-0 lg:border-r">
          <div className="mb-10">
            <div className="text-xs uppercase tracking-[0.4em] text-cyan-300">Chumley</div>
            <h1 className="mt-3 text-3xl font-semibold text-white">TQR Analyser</h1>
            <p className="mt-3 text-sm text-slate-400">Trade quality review, engineer performance and AI-backed image scoring in one workspace.</p>
          </div>
          <nav className="space-y-3">
            {navItems.map((item) => (
              <button
                key={item.key}
                className={`block w-full rounded-2xl px-4 py-3 text-left text-sm font-semibold transition ${view === item.key ? 'bg-cyan-400/15 text-cyan-200 ring-1 ring-cyan-400/30' : 'bg-slate-900/70 text-slate-300 hover:bg-slate-800'}`}
                onClick={() => setView(item.key)}
              >
                {item.label}
              </button>
            ))}
          </nav>
        </aside>

        <main className="flex-1 px-4 py-6 md:px-8 lg:px-10">
          <div className="mb-8 flex flex-wrap items-end justify-between gap-4">
            <div>
              <div className="text-xs uppercase tracking-[0.35em] text-slate-500">Operations Overview</div>
              <div className="mt-2 text-3xl font-semibold text-white">
                {view === 'dashboard' && 'Trade Manager Dashboard'}
                {view === 'appointments' && 'Appointments'}
                {view === 'appointment-detail' && `Appointment ${selectedId || ''}`}
                {view === 'engineers' && 'Engineers'}
                {view === 'engineer-detail' && `${selectedEngineer || ''} Detail`}
                {view === 'tqr-report' && 'TQR Report'}
              </div>
            </div>
          </div>

          {view === 'dashboard' && (
            <DashboardView stats={stats} recentAppointments={recentAppointments} onOpenAppointment={openAppointment} onOpenModal={setModalId} />
          )}
          {view === 'appointments' && (
            <AppointmentsView appointmentsState={appointmentsState} onOpenAppointment={openAppointment} onOpenModal={setModalId} onAnalyse={runAnalysis} analysingIds={analysingIds} />
          )}
          {view === 'appointment-detail' && selectedId && (
            <AppointmentDetailView id={selectedId} onBack={() => setView('appointments')} onOpenEngineer={openEngineer} />
          )}
          {view === 'engineers' && (
            <EngineersView engineers={engineers} onOpenEngineer={openEngineer} />
          )}
          {view === 'engineer-detail' && selectedEngineer && (
            <EngineerDetailView name={selectedEngineer} appointments={allAppointments} onBack={() => setView('engineers')} onOpenAppointment={openAppointment} />
          )}
          {view === 'tqr-report' && (
            <TQRReportView appointments={allAppointments} onAnalyse={runAnalysis} analysingIds={analysingIds} />
          )}
        </main>
      </div>
    </div>
  )
}
