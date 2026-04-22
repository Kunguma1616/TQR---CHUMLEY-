import axios from 'axios'
import { useEffect, useMemo, useRef, useState } from 'react'
import { driver } from 'driver.js'
import 'driver.js/dist/driver.css'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

const api = axios.create({ baseURL: 'http://localhost:8000' })

export const colors = {
  brand: {
    blue: '#27549D',
    yellow: '#F1FF24',
  },
  support: {
    gray: '#848EA3',
    green: '#2EB844',
    orange: '#F29630',
    red: '#D15134',
  },
  primary: {
    light: '#7099DB',
    default: '#27549D',
    darker: '#17325E',
    subtle: '#F7F9FD',
  },
  error: {
    light: '#E49786',
    default: '#D15134',
    darker: '#812F1D',
    subtle: '#FAEDEA',
  },
  warning: {
    light: '#F7C182',
    default: '#F29630',
    darker: '#A35C0A',
    subtle: '#FEF5EC',
  },
  grayscale: {
    title: '#1A1D23',
    body: '#323843',
    subtle: '#646F86',
    caption: '#848EA3',
    negative: '#F3F4F6',
    disabled: '#CDD1DA',
    border: {
      default: '#CDD1DA',
      disabled: '#E8EAEE',
      subtle: '#F3F4F6',
    },
    surface: {
      default: '#CDD1DA',
      disabled: '#E8EAEE',
      subtle: '#F3F4F6',
    },
  },
  border: {
    primary: {
      light: '#7099DB',
      default: '#27549D',
      darker: '#17325E',
      subtle: '#DEE8F7',
    },
    error: {
      light: '#E49786',
      default: '#D15134',
      darker: '#812F1D',
      subtle: '#F6DBD5',
    },
    warning: {
      light: '#F7C182',
      default: '#F29630',
      darker: '#A35C0A',
      subtle: '#FCE9D4',
    },
  },
  surface: {
    primary: {
      default: '#27549D',
      lighter: '#7099DB',
      darker: '#17325E',
      subtle: '#F7F9FD',
    },
    error: {
      default: '#D15134',
      lighter: '#E49786',
      darker: '#812F1D',
      subtle: '#FAEDEA',
    },
    warning: {
      default: '#F29630',
      lighter: '#F7C182',
      darker: '#A35C0A',
      subtle: '#FEF5EC',
    },
  },
  text: {
    primary: {
      label: '#17325E',
    },
    error: {
      label: '#812F1D',
    },
    warning: {
      label: '#A35C0A',
    },
    grayscale: {
      title: '#1A1D23',
      body: '#323843',
      subtle: '#646F86',
      caption: '#848EA3',
      negative: '#F3F4F6',
      disabled: '#CDD1DA',
    },
  },
}

type View = 'dashboard' | 'appointments' | 'appointment-detail'

type TqrField = {
  score?: number
  value?: string
  salesforceValue?: string
  outcome: 'Pass' | 'Review' | 'Fail'
  confidence: number
  evidenceCited: string[]
  rationale: string
  whatWouldIncrease?: string
  whatWouldDecrease?: string
  flaggedIssues?: string[]
  missedOpportunities?: string[]
  urgentIssueDetected?: boolean
  urgentIssueDescription?: string | null
  reviewReason?: string | null
  actualMinutes?: number
  expectedMinutes?: number | null
  benchmarkSource?: 'industry' | 'company' | 'none'
  mandatoryPhotosPresent?: { location: boolean; workBefore: boolean; workAfter: boolean; jobCompletion: boolean }
  }

type TqrFields = {
  customerSignature?: TqrField
  imagesQuality?: TqrField
  paymentAttempted?: TqrField
  report?: TqrField
  timeTaken?: TqrField
  workmanship?: TqrField
  decisionMaking?: TqrField
}

type TqrResult = {
  workmanship: number
  overall: number
  summary: string
  flags: string[]
  recommendation: string
  verdict?: string
  hard_fail?: boolean
  tqr_fields?: TqrFields | null
  image_descriptions?: Array<{ id?: string; title?: string; description: string }>
  analysed_at?: string
}

type Appointment = {
  Id: string
  Trade_Group_Postcode__c?: string
  Trade_Group_Region__c?: string
  Allocated_Engineer__c?: string
  AllocatedEngineerName?: string
  Feedback_Notes__c?: string
  AppointmentNumber?: string
  Status?: string
  Scheduled_Trade__c?: string
  Description?: string
  ActualStartTimeFormatted?: string
  ActualEndTimeFormatted?: string
  Attendance_Notes_for_Office__c?: string
  Attendance_Report_for_Customer__c?: string
  Workmanship__c?: string
  Workmanship1__c?: string
  CCT_Charge_Gross__c?: number
  EPR_Status__c?: string
  Work_Order__c?: string
  Report__c?: string
  Post_Visit_Report_Check__c?: string
  Decision_Making__c?: string
  Payment_Attempted__c?: string
  Sector_Type__c?: string
  SchedStartTimeFormatted?: string
  ArrivalWindowStartTimeFormatted?: string
  Duration?: number
  Street?: string
  City?: string
  PostalCode?: string
  Subject?: string
  tqrResult?: TqrResult | null
  tqrScore?: number | null
  documents?: RelatedDocument[]
  workOrderId?: string | null
  workOrder?: {
    Id: string
    WorkOrderNumber?: string
    Description?: string
    Street?: string
    City?: string
    PostalCode?: string
    WorkType?: { Name?: string }
  } | null
  account?: {
    Id: string
    Name?: string
    Phone?: string
    PersonEmail?: string
    Sector_Type__c?: string
  } | null
  site?: string | null
  accountManager?: string | null
}

type RelatedDocument = {
  id: string
  title: string
  fileType?: string
  contentType?: string
  source?: string
  category?: string
  externalUrl?: string
}

type DashboardStats = {
  totalRecords: number
  totalCCTCharge: number
  avgTQRScore: number | null
  byTradeGroup: Array<{ name: string; count: number }>
}

type WorkOrderImage = {
  id: string
  title: string
  base64: string
  contentType: string
}

const pageStyle: React.CSSProperties = {
  minHeight: '100vh',
  background: '#F5F7FA',
  color: colors.text.grayscale.body,
  fontFamily: "'Montserrat', 'Segoe UI', sans-serif",
}

const cardStyle: React.CSSProperties = {
  background: '#fff',
  borderRadius: 16,
  border: `1px solid ${colors.grayscale.border.subtle}`,
  boxShadow: '0 10px 30px rgba(23,50,94,0.06)',
}

const compactInput: React.CSSProperties = {
  width: '100%',
  border: `1px solid ${colors.grayscale.border.default}`,
  borderRadius: 12,
  padding: '10px 12px',
  fontSize: 11,
  color: colors.text.grayscale.body,
  background: '#fff',
  fontFamily: "'Montserrat', 'Segoe UI', sans-serif",
}

const primaryButton: React.CSSProperties = {
  border: 0,
  borderRadius: 16,
  padding: '14px 18px',
  background: '#445EAF',
  color: '#fff',
  fontWeight: 700,
  fontSize: 14,
  cursor: 'pointer',
  fontFamily: "'Montserrat', 'Segoe UI', sans-serif",
}

const metaLabel: React.CSSProperties = {
  margin: 0,
  fontSize: 10,
  color: colors.text.grayscale.caption,
  fontWeight: 700,
  textTransform: 'uppercase',
  letterSpacing: '0.14em',
}

const tableHead: React.CSSProperties = {
  color: colors.text.primary.label,
  fontSize: 10,
  fontWeight: 700,
  paddingBottom: 12,
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
  textAlign: 'left',
  lineHeight: 1.35,
}

const tableCell: React.CSSProperties = {
  padding: '10px 8px 10px 0',
  borderTop: `1px solid ${colors.grayscale.border.subtle}`,
  fontSize: 11,
  verticalAlign: 'top',
  color: colors.text.grayscale.body,
  lineHeight: 1.5,
  wordBreak: 'break-word',
  whiteSpace: 'normal',
}

const formatMoney = (value?: number | null) => new Intl.NumberFormat('en-GB', { style: 'currency', currency: 'GBP' }).format(value ?? 0)
const formatDate = (value?: string | null) => value || 'N/A'
const getTradeGroupLabel = (appointment: Pick<Appointment, 'Trade_Group_Region__c' | 'Trade_Group_Postcode__c' | 'Scheduled_Trade__c'>) =>
  appointment.Trade_Group_Region__c?.trim() || appointment.Trade_Group_Postcode__c?.trim() || appointment.Scheduled_Trade__c?.trim() || 'Unknown'
const getStartTimeLabel = (appointment: Pick<Appointment, 'ActualStartTimeFormatted' | 'SchedStartTimeFormatted' | 'ArrivalWindowStartTimeFormatted'>) =>
  appointment.ActualStartTimeFormatted || appointment.SchedStartTimeFormatted || appointment.ArrivalWindowStartTimeFormatted
const getScoreBandExplanation = (score?: number) => {
  if (score === undefined || score === null) return null
  if (score >= 9) return { band: '9-10', label: 'Perfect', why: 'The AI judged the evidence to be top-tier: clear documentation, strong finish quality, and no visible concerns.' }
  if (score >= 7) return { band: '7-8', label: 'Good', why: 'The AI judged the work as competent and professional, but not exceptional enough to move into the Perfect band.' }
  if (score >= 5) return { band: '5-6', label: 'Acceptable', why: 'The AI found the work completed, but with enough gaps or concerns that a trade manager should look more closely.' }
  if (score >= 3) return { band: '3-4', label: 'Non Acceptable', why: 'The AI found significant quality concerns, incomplete work, or evidence that falls below a competent standard.' }
  return { band: '0-2', label: 'Urgent Issue', why: 'The AI found a safety concern, damage, or a serious enough issue to trigger the lowest band.' }
}

const getImageQualityBandExplanation = (score?: number) => {
  if (score === undefined || score === null) return null
  if (score >= 9) return { band: '9-10', label: 'Good', why: 'The AI judged the photo set as complete and high quality, with all mandatory coverage present and no meaningful gaps.' }
  if (score >= 7) return { band: '7-8', label: 'Good', why: 'The AI found all mandatory photos present with generally good quality and only minor coverage gaps.' }
  if (score >= 5) return { band: '5-6', label: 'Poor', why: 'The AI found all mandatory photos present, but quality issues or missing situational shots were enough to keep the score in the lower band.' }
  if (score >= 3) return { band: '3-4', label: 'Poor', why: 'The AI found a missing mandatory photo or multiple serious quality issues, so the photo set falls below standard.' }
  return { band: '0-2', label: 'Poor', why: 'The AI found multiple mandatory photos missing or no usable photos at all, which is a fail-level outcome.' }
}

const getDisplayOutcomeForField = (field: TqrField, opts?: { forceReview?: boolean }) => {
  if (field.reviewReason || opts?.forceReview) return 'Review'
  if (field.score !== undefined && field.score !== null) {
    if (field.score <= 2) return 'Fail'
    if (field.score <= 6) return 'Review'
    return 'Pass'
  }
  return field.outcome
}

const getEffectiveSalesforceValueForScore = (key: keyof TqrFields, score?: number) => {
  if (score === undefined || score === null) return undefined
  if (key === 'imagesQuality') {
    if (score >= 9) return 'Perfect'
    if (score >= 7) return 'Good'
    if (score >= 5) return 'Acceptable'
    if (score >= 3) return 'Non Acceptable'
    return 'Urgent Issue'
  }
  if (key === 'workmanship' || key === 'decisionMaking') {
    if (score >= 9) return 'Perfect'
    if (score >= 7) return 'Good'
    if (score >= 5) return 'Acceptable'
    if (score >= 3) return 'Non Acceptable'
    return 'Urgent Issue'
  }
  if (key === 'report') {
    if (score >= 9) return 'Perfect'
    if (score >= 7) return 'Good'
    if (score >= 5) return 'Acceptable'
    return 'Non Acceptable'
  }
  return undefined
}

const scoreGuideContent: Record<string, { purpose: string; evidence: string[]; rubric: Array<{ band: string; meaning: string }>; mapping: string[]; triggers: string[]; note?: string }> = {
  workmanship: {
    purpose: 'Assesses the quality of the physical work performed from photographic and written evidence. This is a desk-based assessment from documentation, not an on-site inspection.',
    evidence: [
      'Before and after photos',
      'Workmanship close-up photos where applicable',
      'Parts and materials used',
      'Job description and engineer notes',
      'Any flags or concerns raised in notes',
      'Customer signature and comments recorded alongside',
    ],
    rubric: [
      { band: '9-10 Perfect', meaning: 'Clear before/after evidence, professional finish, correct materials, no visible concerns.' },
      { band: '7-8 Good', meaning: 'Good standard of work with only minor documentation or finish gaps.' },
      { band: '5-6 Acceptable', meaning: 'Completed work but signs of rushing, cosmetic issues, or material concerns.' },
      { band: '3-4 Non Acceptable', meaning: 'Poor finish, incomplete work, or inappropriate materials.' },
      { band: '0-2 Urgent Issue', meaning: 'Safety concern, damage, fundamentally incorrect approach, or remedial recall risk.' },
    ],
    mapping: ['9-10 = Perfect', '7-8 = Good', '5-6 = Acceptable', '3-4 = Non Acceptable', '0-2 = Urgent Issue'],
    triggers: [
      'Insufficient photo evidence',
      'Possible quality issue but AI confidence is low',
      'Visible damage, safety concern, inappropriate material, or unusual approach',
      'Engineer notes flag an unresolved issue',
      'Missing customer signature with thin documentation',
    ],
    note: 'The AI is biased toward Good (7-8) for competent jobs and reserves Perfect for genuinely outstanding evidence.',
  },
  decisionMaking: {
    purpose: 'Assesses the quality of judgment calls the engineer made during the visit: whether they diagnosed correctly, chose an appropriate approach, raised further works where needed, generated quotes for additional opportunities, and managed customer expectations.',
    evidence: [
      'Job description - both the original problem and the diagnosis reached',
      'Attendance Report for Customer and Attendance Notes for Office',
      'Parts used and whether they match the problem',
      'Further works records raised from this visit',
      'Quotes generated during or after the visit',
      'Notes on scope changes, including what was found versus what was expected',
      'Customer interactions recorded in notes',
      'Time Taken signal, where very short times may indicate insufficient investigation',
    ],
    rubric: [
      { band: '9-10 Perfect', meaning: 'Correct diagnosis, appropriate approach, commercial opportunities captured, and strong customer expectation management are visible in the notes.' },
      { band: '7-8 Good', meaning: 'Sound decisions throughout. A minor opportunity may have been missed, but nothing materially wrong with the approach taken.' },
      { band: '5-6 Acceptable', meaning: 'Adequate decisions overall, but there may be a missed value-add opportunity or a sub-optimal diagnostic approach even if the final outcome was broadly correct.' },
      { band: '3-4 Non Acceptable', meaning: 'Significant further works were needed but not raised, expectations were poorly managed, or the chosen approach was wrong enough to cost time, money, or trust.' },
      { band: '0-2 Urgent Issue', meaning: 'A decision created real risk to the customer, engineer, or company, or a serious escalation/safety issue was missed.' },
    ],
    mapping: [
      '9-10 = Perfect',
      '7-8 = Good',
      '5-6 = Acceptable',
      '3-4 = Non Acceptable',
      '0-2 = Urgent Issue',
    ],
    triggers: [
      'Complex trade-specific judgment that AI cannot assess confidently without domain expertise',
      'Engineer notes suggest a scope change or customer issue requiring human interpretation',
      'The job involved a repair-versus-replace decision that is not fully explained in the notes',
      'Further works or quotes seem likely to have been appropriate but were not raised',
      'Customer expectation may have been mismanaged, including promises about scope or timing that may not be deliverable',
    ],
    note: 'Decision Making is highly subjective. The AI should surface concrete observations from the engineer report and photos, but the Trade Manager remains the final reviewer.',
  },
  imagesQuality: {
    purpose: 'Assesses whether photographic evidence of the job is complete, relevant, and of sufficient quality to document what was done. Photos are the primary evidence source for almost every other TQR field, so this field affects confidence across the wider review.',
    evidence: [
      'All images attached to the service appointment',
      'Trade group and job type, to determine which situational photos apply',
      'Job description, to understand what should have been documented visually',
      'Mandatory photo checks: location, work before, work after, and job completion',
      'Situational photo checks: work during, workmanship close-up, and protection used where applicable',
    ],
    rubric: [
      { band: '9-10 Perfect', meaning: 'All four mandatory photos are present with excellent quality, and applicable situational photos are also present with no meaningful coverage gaps.' },
      { band: '7-8 Good', meaning: 'All four mandatory photos are present with good quality. Most applicable situational photos are present with only minor quality or coverage gaps.' },
      { band: '5-6 Acceptable', meaning: 'All four mandatory photos are present, but one or more has quality issues or some applicable situational photos are missing.' },
      { band: '3-4 Non Acceptable', meaning: 'One mandatory photo is missing, or multiple mandatory photos are too unclear, dark, blurry, or poorly framed to document the work properly.' },
      { band: '0-2 Urgent Issue', meaning: 'Multiple mandatory photos are missing, or zero photos are attached. This is a fail-level outcome.' },
    ],
    mapping: [
      '9-10 = Perfect',
      '7-8 = Good',
      '5-6 = Acceptable',
      '3-4 = Non Acceptable',
      '0-2 = Urgent Issue',
    ],
    triggers: [
      'Photos are present but the subject is unclear',
      'Photos look duplicated instead of showing progression',
      'A situational photo may be missing but applicability is unclear',
      'Photo timing suggests images were taken far before or after the job',
    ],
    note: 'A job must have all 4 mandatory photos to score 5 or above. If only 1, 2, or 3 mandatory photos are available, Image Quality should stay in the low band.',
  },
  report: {
    purpose: 'Assesses the quality of the written job report - specifically whether the written record is detailed enough for someone unfamiliar with the job to understand what was found, what was done, what parts were used, and what needs to happen next.',
    evidence: [
      'Job description and notes field',
      'Works Completion Summary / Attendance Report for Customer field',
      'Parts and materials listed',
      'Comments for Projected Difference, where applicable',
      'Reason for Projected Difference, where applicable',
    ],
    rubric: [
      { band: '9-10 Perfect', meaning: 'Clear problem statement, complete work summary, all parts listed, explicit next steps where applicable. Professional tone. A future engineer reading this could fully understand what happened.' },
      { band: '7-8 Good', meaning: 'Clear problem and work summary. One minor element is missing or thin, such as next steps being implied rather than stated, or parts being listed but not quantified.' },
      { band: '5-6 Acceptable', meaning: 'Describes what was done but is thin on detail, context, or diagnosis reasoning. A future engineer would need to piece things together.' },
      { band: '3-4 Non Acceptable', meaning: 'One-line report, major structural gaps, or repeated template language without enough job-specific content.' },
      { band: '0-2 Non Acceptable', meaning: 'Notes are absent, completely incomprehensible, or effectively just copy the booking notes with nothing meaningful added.' },
    ],
    mapping: [
      '9-10 = Perfect',
      '7-8 = Good',
      '5-6 = Acceptable',
      '0-4 = Non Acceptable',
    ],
    triggers: [
      'Notes contain a flag, claim, or complaint that appears to need follow-up but was not resolved',
      'Notes mention a scope change, customer disagreement, or site issue without resolution',
      'Template or boilerplate wording appears instead of a job-specific report',
      'Notes describe work done but the listed parts do not match the written report',
    ],
    note: 'Text analysis is usually reliable for report completeness, but Trade Manager confirmation is still required because AI cannot fully assess trade-specific technical accuracy.',
  },
  timeTaken: {
    purpose: 'Assesses whether time spent on site was appropriate for the work completed. Too quick suggests corner-cutting; too long suggests inefficiency, over-servicing, or padding. This field is benchmark-dependent and should surface as review-led when no benchmark exists.',
    evidence: [
      'Actual Start time on the service appointment',
      'Actual End time on the service appointment',
      'Trade group and scheduled trade',
      'Job description and scope of actual work performed',
      'Industry benchmark where available',
      'Historical company benchmark where available',
    ],
    rubric: [
      { band: '9-10 Ideal', meaning: 'Within 10% of expected time for the specific trade and job type. Suggests confident, efficient work at the right pace.' },
      { band: '7-8 Good', meaning: 'Within 25% of expected time. Mild over- or under-run but nothing concerning.' },
      { band: '5-6 Acceptable', meaning: 'Within 50% of expected time. Some justification is reasonable but worth noting.' },
      { band: '3-4 Concerning', meaning: 'Outside 50% range. Likely rushed or inefficient, and the notes should explain why.' },
      { band: '0-2 Extreme outlier', meaning: 'Dramatic over- or under-run. Almost always needs a conversation with the engineer.' },
    ],
    mapping: [
      '7-10 = Ideal',
      '3-6 over expected = Excessive',
      '3-6 under expected = Rushed',
      '0-2 over expected = Excessive (severe)',
      '0-2 under expected = Rushed (severe)',
    ],
    triggers: [
      'No benchmark exists for this trade and job type',
      'The job type is bespoke or unusual so benchmark confidence is low',
      'The duration is an extreme outlier in either direction',
      'Actual times appear inconsistent, such as start after end or zero duration',
    ],
    note: 'At launch, Time on Site should usually surface as Review when no benchmark exists. It should not present as a confident Pass without expected-time context.',
  },
}
const escapeHtml = (value?: string | number | null) =>
  String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;')
const hasDisplayValue = (value?: string | number | null) => {
  if (value === null || value === undefined) return false
  if (typeof value === 'number') return true
  const normalised = String(value).replaceAll('—', '').replaceAll('-', '').trim()
  return normalised.length > 0
}

const StatusBadge = ({ status }: { status?: string }) => {
  let background = colors.primary.subtle
  let color = colors.primary.default
  if (status === 'Visit Complete') {
    background = '#ECF8EF'
    color = colors.support.green
  } else if (status === 'Cancelled') {
    background = colors.error.subtle
    color = colors.error.default
  } else if (status === 'In Progress') {
    background = colors.warning.subtle
    color = colors.warning.default
  }
  return <span style={{ display: 'inline-flex', borderRadius: 999, padding: '5px 10px', background, color, fontWeight: 700, fontSize: 11 }}>{status || 'Unknown'}</span>
}

const StatCard = ({ title, value, note }: { title: string; value: string; note: string }) => (
  <div style={{ ...cardStyle, padding: 18 }}>
    <div style={{ maxWidth: 320 }}>
      <p style={metaLabel}>{title}</p>
      <div style={{ marginTop: 10, fontSize: 28, fontWeight: 800, color: colors.grayscale.title }}>{value}</div>
      {note.trim() ? <div style={{ marginTop: 8, color: colors.text.grayscale.subtle, fontSize: 12, lineHeight: 1.5 }}>{note}</div> : null}
    </div>
  </div>
)

const DetailRow = ({ label, value }: { label: string; value?: string | number | null }) => (
  <div style={{ ...cardStyle, padding: 14 }}>
    <p style={metaLabel}>{label}</p>
    <div style={{ marginTop: 8, fontSize: 12, lineHeight: 1.6 }}>{value || 'N/A'}</div>
  </div>
)

const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null
  return (
    <div style={{
      background: '#fff',
      border: `1px solid ${colors.grayscale.border.default}`,
      borderRadius: 8,
      padding: '8px 14px',
      boxShadow: '0 4px 16px rgba(39,84,157,0.10)',
    }}>
      <p style={{ margin: 0, fontSize: 11, color: colors.text.grayscale.subtle, fontWeight: 500 }}>{label}</p>
      <p style={{ margin: '4px 0 0', fontSize: 14, fontWeight: 700, color: colors.primary.darker }}>
        {payload[0].value.toLocaleString()} <span style={{ fontWeight: 400, fontSize: 10 }}>jobs</span>
      </p>
    </div>
  )
}

const barColor = (value: number, max: number) => {
  const ratio = value / max
  if (ratio > 0.6) return colors.primary.darker
  if (ratio > 0.3) return colors.primary.default
  if (ratio > 0.1) return colors.primary.light
  return colors.grayscale.disabled
}

function TQRTradeChart({
  data,
  visibleRows = 14,
}: {
  data: Array<{ name: string; count: number }>
  visibleRows?: number
}) {
  const rows = [...data].sort((a, b) => b.count - a.count).slice(0, visibleRows)
  const chartHeight = 420

  return (
    <div style={{ ...cardStyle, padding: '8px 12px 12px', borderRadius: 0, boxShadow: 'none' }}>
      <ResponsiveContainer width="100%" height={chartHeight}>
        <BarChart data={rows} margin={{ top: 8, right: 14, bottom: 118, left: 10 }}>
          <CartesianGrid stroke={colors.grayscale.border.subtle} vertical={false} />
          <XAxis
            dataKey="name"
            stroke={colors.text.grayscale.caption}
            angle={-45}
            textAnchor="end"
            interval={0}
            height={124}
            tick={{ fontSize: 8, fill: colors.text.grayscale.body, fontFamily: 'inherit' }}
            tickLine={false}
            axisLine={false}
          />
          <YAxis
            stroke={colors.text.grayscale.caption}
            tick={{ fontSize: 10, fill: colors.text.grayscale.body, fontFamily: 'inherit' }}
            tickLine={false}
            axisLine={false}
            label={{ value: 'Record Count', angle: -90, position: 'insideLeft', style: { fill: colors.text.grayscale.title, fontSize: 10 } }}
          />
          <Tooltip content={<CustomTooltip />} cursor={{ fill: colors.primary.subtle }} />
          <Bar dataKey="count" fill="#5B9BD5" radius={[0, 0, 0, 0]} barSize={44}>
            {rows.map((entry) => (
              <Cell key={entry.name} fill="#5B9BD5" />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <div style={{ textAlign: 'center', color: colors.text.grayscale.title, fontSize: 10, marginTop: -8 }}>
        Top Trade Group Postcodes
      </div>
    </div>
  )
}

function DashboardView({ stats }: { stats: DashboardStats | null }) {
  const tradeGroupChartData = useMemo(
    () => stats?.byTradeGroup ?? [],
    [stats],
  )

  return (
    <div style={{ display: 'grid', gap: 20 }}>
      <div style={{ display: 'grid', gap: 14, gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))' }}>
        <StatCard title="Not Checked By TM's" value={`${stats?.totalRecords ?? 0}`} note="Visit complete jobs from the last 90 days still waiting for Trade Manager review." />
      </div>
      <TQRTradeChart data={tradeGroupChartData} />
    </div>
  )
}

function AppointmentsView({
  appointments,
  total,
  page,
  totalPages,
  loading,
  filters,
  setFilters,
  setPage,
  onOpenAppointment,
  onOpenWorkOrder,
}: {
  appointments: Appointment[]
  total: number
  page: number
  totalPages: number
  loading: boolean
  filters: { search: string; engineer: string; trade: string; sector: string }
  setFilters: React.Dispatch<React.SetStateAction<{ search: string; engineer: string; trade: string; sector: string }>>
  setPage: React.Dispatch<React.SetStateAction<number>>
  onOpenAppointment: (id: string) => void
  onOpenWorkOrder: (workOrderId: string | null | undefined) => void
}) {
  return (
    <div style={{ display: 'grid', gap: 20 }}>
      <div style={{ ...cardStyle, padding: 14, display: 'grid', gap: 10, gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>
        <input style={compactInput} placeholder="Search queue" value={filters.search} onChange={(e) => setFilters((s) => ({ ...s, search: e.target.value }))} />
        <input style={compactInput} placeholder="Filter engineer" value={filters.engineer} onChange={(e) => setFilters((s) => ({ ...s, engineer: e.target.value }))} />
        <input style={compactInput} placeholder="Filter trade" value={filters.trade} onChange={(e) => setFilters((s) => ({ ...s, trade: e.target.value }))} />
        <input style={compactInput} placeholder="Filter sector type" value={filters.sector} onChange={(e) => setFilters((s) => ({ ...s, sector: e.target.value }))} />
      </div>

      <div style={{ ...cardStyle, padding: 16, overflow: 'hidden' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 18, alignItems: 'center', marginBottom: 12 }}>
          <div>
            <p style={metaLabel}>Post Visit Report - Last 90 Days</p>
            <div style={{ marginTop: 4, color: colors.primary.darker, fontSize: 18, fontWeight: 800 }}>Not Checked By TM&apos;s</div>
          </div>
          <div style={{ color: colors.text.grayscale.subtle, fontSize: 12 }}>{loading ? 'Loading...' : `${total} jobs`}</div>
        </div>

        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: 1320, tableLayout: 'fixed' }}>
            <colgroup>
              <col style={{ width: 130 }} />
              <col style={{ width: 120 }} />
              <col style={{ width: 160 }} />
              <col style={{ width: 105 }} />
              <col style={{ width: 92 }} />
              <col style={{ width: 98 }} />
              <col style={{ width: 200 }} />
              <col style={{ width: 90 }} />
              <col style={{ width: 170 }} />
              <col style={{ width: 86 }} />
              <col style={{ width: 98 }} />
              <col style={{ width: 96 }} />
              <col style={{ width: 84 }} />
              <col style={{ width: 122 }} />
              <col style={{ width: 84 }} />
              <col style={{ width: 112 }} />
              <col style={{ width: 88 }} />
              <col style={{ width: 88 }} />
            </colgroup>
            <thead>
              <tr>
                <th style={tableHead}>Trade Group Postcode</th>
                <th style={tableHead}>Allocated Engineer</th>
                <th style={tableHead}>Feedback Notes</th>
                <th style={tableHead}>Appointment Number</th>
                <th style={tableHead}>Status</th>
                <th style={tableHead}>Scheduled Trade</th>
                <th style={tableHead}>Description</th>
                <th style={tableHead}>Actual End</th>
                <th style={tableHead}>Attendance Report for Customer</th>
                <th style={tableHead}>Workmanship</th>
                <th style={tableHead}>Workmanship (2nd)</th>
                <th style={tableHead}>CCT Charge Gross</th>
                <th style={tableHead}>EPR Status</th>
                <th style={tableHead}>Work Order</th>
                <th style={tableHead}>Report</th>
                <th style={tableHead}>Post Visit Report Check</th>
                <th style={tableHead}>Decision Making</th>
                <th style={tableHead}>Payment Attempted</th>
              </tr>
            </thead>
            <tbody>
              {appointments.map((row) => (
                <tr key={row.Id}>
                  <td style={{ ...tableCell, fontWeight: 600 }}>{getTradeGroupLabel(row)}</td>
                  <td style={tableCell}>{row.AllocatedEngineerName || 'Unassigned'}</td>
                  <td style={tableCell}>{row.Feedback_Notes__c || 'No feedback'}</td>
                  <td style={{ ...tableCell, color: colors.primary.default, fontWeight: 800, cursor: 'pointer' }} onClick={() => onOpenAppointment(row.Id)}>{row.AppointmentNumber || 'N/A'}</td>
                  <td style={tableCell}><StatusBadge status={row.Status} /></td>
                  <td style={tableCell}>{row.Scheduled_Trade__c || 'Not set'}</td>
                  <td style={tableCell}>{row.Description || 'No description'}</td>
                  <td style={tableCell}>{formatDate(row.ActualEndTimeFormatted)}</td>
                  <td style={tableCell}>{row.Attendance_Report_for_Customer__c || 'No attendance report'}</td>
                  <td style={tableCell}>{row.Workmanship__c || 'Not scored'}</td>
                  <td style={tableCell}>{row.Workmanship1__c || 'Not scored'}</td>
                  <td style={tableCell}>{formatMoney(row.CCT_Charge_Gross__c)}</td>
                  <td style={tableCell}>{row.EPR_Status__c || 'Pending'}</td>
                  <td style={{ ...tableCell, color: row.workOrderId ? colors.primary.default : colors.text.grayscale.body, fontWeight: 700, cursor: row.workOrderId ? 'pointer' : 'default' }} onClick={() => row.workOrderId && onOpenWorkOrder(row.workOrderId)}>
                    {row.Work_Order__c || row.workOrder?.WorkOrderNumber || 'Not linked'}
                  </td>
                  <td style={tableCell}>{row.Report__c || 'Pending'}</td>
                  <td style={tableCell}>{row.Post_Visit_Report_Check__c || 'Pending TM Review'}</td>
                  <td style={tableCell}>{row.Decision_Making__c || 'Pending'}</td>
                  <td style={tableCell}>{row.Payment_Attempted__c || 'Pending'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 16 }}>
          <button style={primaryButton} disabled={page <= 1} onClick={() => setPage((p) => Math.max(1, p - 1))}>Previous</button>
          <div style={{ color: colors.text.grayscale.subtle, fontSize: 12 }}>Page {page} of {totalPages}</div>
          <button style={primaryButton} disabled={page >= totalPages} onClick={() => setPage((p) => Math.min(totalPages, p + 1))}>Next</button>
        </div>
      </div>
    </div>
  )
}

function AppointmentDetailView({
  appointment,
  workOrderImages,
  imagesLoading,
  imagesError,
  tqrResult,
  analyseLoading,
  analyseError,
  onBack,
  onOpenWorkOrder,
  onRunAnalysis,
  onReanalyse,
  onDownloadReport,
}: {
  appointment: Appointment | null
  workOrderImages: WorkOrderImage[]
  imagesLoading: boolean
  imagesError: string | null
  tqrResult: TqrResult | null
  analyseLoading: boolean
  analyseError: string | null
  onBack: () => void
  onOpenWorkOrder: (id: string | null | undefined) => void
  onRunAnalysis: (id: string) => void
  onReanalyse: (id: string) => void
  onDownloadReport: () => void
}) {
  const lastFetchedId = useRef<string | null>(null)
  const [selectedImage, setSelectedImage] = useState<WorkOrderImage | null>(null)
  const [imageDescriptionOverrides, setImageDescriptionOverrides] = useState<Record<string, string>>({})

  useEffect(() => {
    const wid = appointment?.workOrderId
    if (wid && wid !== lastFetchedId.current) {
      lastFetchedId.current = wid
      onOpenWorkOrder(wid)
    }
  }, [appointment?.workOrderId])

  const needsFreshImageDescription = (description?: string | null) =>
    !description
    || /could not be generated/i.test(description)
    || /run or re-run/i.test(description)

  useEffect(() => {
    const workOrderId = appointment?.workOrderId
    const image = selectedImage
    if (!workOrderId || !image) return
    const cachedDescription =
      imageDescriptionOverrides[image.id]
      || (tqrResult?.image_descriptions || []).find((item) => item.id === image.id || item.title === image.title)?.description
      || null
    if (!needsFreshImageDescription(cachedDescription)) return

    let cancelled = false
    ;(async () => {
      try {
        const { data } = await api.get(`/api/work-orders/${workOrderId}/images/${image.id}/describe`)
        const description = String(data?.description || '').trim()
        if (!cancelled && description) {
          setImageDescriptionOverrides((prev) => ({ ...prev, [image.id]: description }))
        }
      } catch {
        // Leave the existing fallback text in place if live description fails.
      }
    })()

    return () => {
      cancelled = true
    }
  }, [appointment?.workOrderId, selectedImage, tqrResult, imageDescriptionOverrides])

  if (!appointment) return <div style={{ ...cardStyle, padding: 32 }}>Loading...</div>

  const workOrderLabel = appointment.workOrder?.WorkOrderNumber || appointment.Work_Order__c || 'Not linked'
  const hasWorkOrder = Boolean(appointment.workOrderId)

  const Section = ({ title, accent, children }: { title: string; accent?: string; children: React.ReactNode }) => (
    <div style={{ ...cardStyle, padding: '28px 32px', borderTop: `3px solid ${accent || colors.primary.darker}` }}>
      <div style={{ fontSize: 11, fontWeight: 800, letterSpacing: '0.16em', textTransform: 'uppercase', color: accent || colors.primary.darker, marginBottom: 20 }}>{title}</div>
      {children}
    </div>
  )

  const Field = ({ label, value, wide }: { label: string; value?: string | null; wide?: boolean }) =>
  hasDisplayValue(value) ? (
    <div style={{ gridColumn: wide ? 'span 2' : undefined }}>
      <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: colors.text.grayscale.caption, marginBottom: 5 }}>{label}</div>
      <div style={{ fontSize: 13, color: colors.text.grayscale.body, lineHeight: 1.6 }}>{value}</div>
    </div>
  ) : null

  const grid = (cols = 4) => ({ display: 'grid', gap: '20px 32px', gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` } as React.CSSProperties)

  const outcomeColor = (o?: string) => o === 'Pass' ? '#16a34a' : o === 'Review' ? '#d97706' : '#dc2626'
  const outcomeBackground = (o?: string) => o === 'Pass' ? '#f0fdf4' : o === 'Review' ? '#fffbeb' : '#fef2f2'
  const imageDescriptions = tqrResult?.image_descriptions || []
  const documents = appointment.documents || []
  const openDocument = (doc: RelatedDocument) => {
    if (doc.externalUrl) {
      window.open(doc.externalUrl, '_blank', 'noopener,noreferrer')
      return
    }
    const base = String(api.defaults.baseURL || '').replace(/\/$/, '')
    window.open(`${base}/api/content/${doc.id}?inline=true`, '_blank', 'noopener,noreferrer')
  }
  const getImageDescription = (image: WorkOrderImage) =>
    imageDescriptionOverrides[image.id]
    || imageDescriptions.find((item) => item.id === image.id || item.title === image.title)?.description
    || null
  const startScoreGuide = () => {
    const tour = driver({
      showProgress: true,
      animate: true,
      steps: [
        {
          element: '[data-driver="tqr-summary"]',
          popover: {
            title: 'TQR score summary',
            description: 'This top bar shows the overall verdict, weighted total score, hard-fail status, and when the AI last analysed the job.',
          },
        },
        {
          element: '[data-driver="scorecard-workmanship"]',
          popover: {
            title: 'Workmanship score',
            description: 'Workmanship is weighted at 20%. The AI scores it from photos, notes, materials, and any visible risks, then maps that numeric score to the Salesforce picklist.',
          },
        },
        {
          element: '[data-driver="workmanship-guide"]',
          popover: {
            title: 'How the workmanship score is calculated',
            description: 'This panel lists the evidence sources, scoring bands, Salesforce mapping, review triggers, and the bias toward Good unless evidence is genuinely exceptional.',
          },
        },
        {
          element: '[data-driver="score-evidence"]',
          popover: {
            title: 'Evidence used',
            description: 'Evidence cited shows the actual observations that drove the score. This is the main explanation the trade manager should review.',
          },
        },
      ],
    })
    tour.drive()
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      {/* Top bar */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 12 }}>
        <button
          onClick={onBack}
          style={{ background: 'transparent', border: `1px solid ${colors.grayscale.border.default}`, borderRadius: 8, padding: '8px 18px', fontSize: 13, fontWeight: 600, color: colors.text.grayscale.body, cursor: 'pointer', fontFamily: 'inherit' }}
        >
          Back to Queue
        </button>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <button
            onClick={onDownloadReport}
            style={{ background: colors.primary.darker, border: 'none', borderRadius: 8, padding: '8px 16px', fontSize: 12, fontWeight: 700, color: '#fff', cursor: 'pointer', fontFamily: 'inherit' }}
          >
            Download PDF Report
          </button>
          <StatusBadge status={appointment.Status} />
          <span style={{ fontSize: 13, fontWeight: 700, color: colors.primary.darker }}>{appointment.AppointmentNumber || ''}</span>
        </div>
      </div>

      {/* Hero header */}
      <div style={{ ...cardStyle, background: `linear-gradient(135deg, ${colors.primary.darker} 0%, ${colors.primary.default} 100%)`, color: '#fff', padding: '28px 32px', borderTop: 'none' }}>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '20px 40px' }}>
          {[
            { label: 'Customer', value: appointment.account?.Name },
            { label: 'Work Type', value: appointment.workOrder?.WorkType?.Name || appointment.Scheduled_Trade__c },
            { label: 'Engineer', value: appointment.AllocatedEngineerName },
            { label: 'Actual End', value: appointment.ActualEndTimeFormatted },
            { label: 'Work Order', value: workOrderLabel },
          ].map(({ label, value }) => (
            <div key={label}>
              <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: '0.18em', textTransform: 'uppercase', color: 'rgba(255,255,255,0.55)', marginBottom: 4 }}>{label}</div>
              <div style={{ fontSize: 14, fontWeight: 700, color: '#fff' }}>{value || ''}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Customer Detail */}
      <Section title="Customer Detail">
        <div style={grid(4)}>
          <Field label="Account" value={appointment.account?.Name} />
          <Field label="Account Manager" value={appointment.accountManager} />
          <Field label="Site" value={appointment.site} />
          <Field label="Address" value={[appointment.Street, appointment.City, appointment.PostalCode].filter(Boolean).join(', ')} />
          <Field label="Phone" value={appointment.account?.Phone} />
          <Field label="Email" value={appointment.account?.PersonEmail} />
        </div>
      </Section>

      {/* Booking Detail */}
      <Section title="Booking Detail">
        <div style={grid(4)}>
          <div>
            <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: colors.text.grayscale.caption, marginBottom: 5 }}>Work Order</div>
            <button
              style={{ background: 'none', border: 'none', padding: 0, fontSize: 13, fontWeight: 700, color: hasWorkOrder ? colors.primary.default : colors.text.grayscale.body, cursor: hasWorkOrder ? 'pointer' : 'default', fontFamily: 'inherit', textDecoration: hasWorkOrder ? 'underline' : 'none', textUnderlineOffset: 3 }}
              onClick={() => hasWorkOrder && onOpenWorkOrder(appointment.workOrderId)}
            >{workOrderLabel}</button>
          </div>
          <Field label="Work Type" value={appointment.workOrder?.WorkType?.Name} />
          <Field label="Scheduled Trade" value={appointment.Scheduled_Trade__c} />
          <Field label="Trade Group" value={getTradeGroupLabel(appointment)} />
          <Field label="Sector Type" value={appointment.Sector_Type__c} />
          <Field label="Allocated Engineer" value={appointment.AllocatedEngineerName} />
          <Field label="Scheduled Start" value={appointment.SchedStartTimeFormatted} />
          <Field label="Arrival Window" value={appointment.ArrivalWindowStartTimeFormatted} />
          <Field label="Actual End" value={appointment.ActualEndTimeFormatted} />
          <Field label="Duration" value={appointment.Duration ? `${appointment.Duration} mins` : undefined} />
          <Field label="Scope of Works" value={appointment.workOrder?.Description} wide />
        </div>
      </Section>

      {/* Engineer Job Report */}
      <Section title="Engineer Job Report" accent="#0f766e">
        <div style={{ display: 'grid', gap: 16, gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', marginBottom: 20 }}>
          {[
            { label: 'Actual Start', value: getStartTimeLabel(appointment) },
            { label: 'Actual End', value: appointment.ActualEndTimeFormatted },
          ].filter(({ value }) => hasDisplayValue(value)).map(({ label, value }) => (
            <div key={label} style={{ background: '#f0fdf4', borderRadius: 10, padding: '14px 16px', border: '1px solid #bbf7d0' }}>
              <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: '#0f766e', marginBottom: 6 }}>{label}</div>
              <div style={{ fontSize: 14, fontWeight: 700, color: colors.text.grayscale.title }}>{value}</div>
            </div>
          ))}
        </div>
        <div style={{ display: 'grid', gap: '20px 32px', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))' }}>
          {[
            { label: 'Attendance Notes for Office', value: appointment.Attendance_Notes_for_Office__c },
            { label: 'Attendance Report', value: appointment.Attendance_Report_for_Customer__c },
            { label: 'Feedback Notes', value: appointment.Feedback_Notes__c },
            { label: 'Description', value: appointment.Description },
          ].filter(({ value }) => hasDisplayValue(value)).map(({ label, value }) => (
            <div key={label} style={{ background: '#f8fafc', borderRadius: 10, padding: '16px 18px' }}>
              <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: '#0f766e', marginBottom: 8 }}>{label}</div>
              <div style={{ fontSize: 13, color: colors.text.grayscale.body, lineHeight: 1.7 }}>{value}</div>
            </div>
          ))}
        </div>
      </Section>

      <Section title="Related Documents" accent="#7c3aed">
        {documents.length === 0 ? (
          <div style={{ fontSize: 13, color: colors.text.grayscale.subtle }}>No invoice, payment, or service report documents found.</div>
        ) : (
          <div style={{ display: 'grid', gap: 12 }}>
            {documents.map((doc) => (
              <div key={doc.id} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 16, padding: '14px 16px', borderRadius: 10, background: '#faf5ff', border: '1px solid #e9d5ff' }}>
                <div style={{ minWidth: 0 }}>
                  <div style={{ fontSize: 13, fontWeight: 700, color: colors.text.grayscale.title }}>{doc.title}</div>
                  <div style={{ fontSize: 11, color: colors.text.grayscale.caption, marginTop: 4, textTransform: 'capitalize' }}>
                    {doc.category || 'document'}{doc.fileType ? ` - ${doc.fileType}` : ''}{doc.source ? ` - ${doc.source}` : ''}
                  </div>
                </div>
                <button
                  onClick={() => openDocument(doc)}
                  style={{ background: colors.primary.darker, color: '#fff', border: 'none', borderRadius: 8, padding: '8px 14px', fontSize: 12, fontWeight: 700, cursor: 'pointer', whiteSpace: 'nowrap' }}
                >
                  Open
                </button>
              </div>
            ))}
          </div>
        )}
      </Section>

      {/* TQR AI Analysis */}
      <Section title="TQR AI Analysis" accent={colors.primary.darker}>
        {!tqrResult && !analyseLoading && (
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', padding: '32px 0', gap: 14 }}>
            <div style={{ fontSize: 13, color: colors.text.grayscale.subtle, textAlign: 'center', maxWidth: 400 }}>
              AI will analyse all 7 TQR quality fields using job notes, evidence, and photos
            </div>
            <button
              style={{ background: colors.primary.darker, color: '#fff', border: 'none', borderRadius: 10, padding: '12px 32px', fontSize: 14, fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit', letterSpacing: '0.02em' }}
              onClick={() => appointment?.Id && onRunAnalysis(appointment.Id)}
            >Run TQR Analysis</button>
          </div>
        )}
        {analyseLoading && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 14, padding: '24px 0' }}>
            <div style={{ width: 18, height: 18, border: `2px solid ${colors.primary.darker}`, borderTopColor: 'transparent', borderRadius: '50%', animation: 'spin 0.8s linear infinite' }} />
            <span style={{ fontSize: 13, color: colors.text.grayscale.subtle }}>Analysing - this may take 20-40 seconds...</span>
          </div>
        )}
        {analyseError && <div style={{ background: '#fef2f2', border: '1px solid #fecaca', borderRadius: 8, padding: '12px 16px', fontSize: 13, color: '#dc2626' }}>{analyseError}</div>}
        {tqrResult && (() => {
          const isPass = tqrResult.verdict === 'TQR'
          const isSub = tqrResult.verdict === 'Sub standard'
          const verdictBg = isPass ? '#16a34a' : isSub ? '#d97706' : '#dc2626'
          const fields = tqrResult.tqr_fields
          const fieldDefs: { key: keyof TqrFields; label: string; weight: string }[] = [
            { key: 'workmanship', label: 'Workmanship', weight: '20%' },
            { key: 'decisionMaking', label: 'Decision Making', weight: '20%' },
            { key: 'imagesQuality', label: 'Image Quality', weight: '20%' },
            { key: 'report', label: 'Report', weight: '15%' },
            { key: 'timeTaken', label: 'Time on Site', weight: '10%' },
            { key: 'paymentAttempted', label: 'Payment', weight: '10%' },
            { key: 'customerSignature', label: 'Signature', weight: '5%' },
          ]
          return (
            <div>
              {/* Score summary bar */}
              <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 24, padding: '18px 24px', background: '#f8fafc', borderRadius: 12, flexWrap: 'wrap' }}>
              <div data-driver="tqr-summary" style={{ display: 'contents' }} />
                <div style={{ background: verdictBg, color: '#fff', fontWeight: 800, fontSize: 13, padding: '6px 18px', borderRadius: 8, letterSpacing: '0.04em' }}>{tqrResult.verdict}</div>
                <div style={{ fontSize: 32, fontWeight: 800, color: verdictBg, lineHeight: 1 }}>{tqrResult.overall}<span style={{ fontSize: 16, fontWeight: 500, color: colors.text.grayscale.caption }}>/10</span></div>
                {tqrResult.hard_fail && <div style={{ background: '#fef2f2', border: '1px solid #fecaca', color: '#dc2626', fontSize: 11, fontWeight: 700, padding: '4px 12px', borderRadius: 6 }}>Hard Fail Triggered</div>}
                <div style={{ marginLeft: 'auto', display: 'flex', gap: 10 }}>
                  <button style={{ background: colors.primary.subtle, border: `1px solid ${colors.border.primary.subtle}`, borderRadius: 6, padding: '4px 12px', fontSize: 11, fontWeight: 700, color: colors.primary.darker, cursor: 'pointer', fontFamily: 'inherit' }} onClick={startScoreGuide}>How scoring works</button>
                  <span style={{ fontSize: 11, color: colors.text.grayscale.caption }}>{tqrResult.analysed_at}</span>
                  <button style={{ background: 'transparent', border: `1px solid ${colors.grayscale.border.default}`, borderRadius: 6, padding: '4px 12px', fontSize: 11, fontWeight: 600, color: colors.text.grayscale.body, cursor: 'pointer', fontFamily: 'inherit' }} onClick={() => appointment?.Id && onReanalyse(appointment.Id)}>Re-analyse</button>
                </div>
              </div>

              {/* Summary */}
              {tqrResult.summary && (
                <div style={{ fontSize: 13, color: colors.text.grayscale.body, marginBottom: 20, lineHeight: 1.7, padding: '14px 20px', borderLeft: `3px solid ${colors.primary.light}`, background: colors.primary.subtle, borderRadius: '0 8px 8px 0' }}>{tqrResult.summary}</div>
              )}

              {/* Flags */}
              {(tqrResult.flags?.length ?? 0) > 0 && (
                <div style={{ marginBottom: 20, display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {tqrResult.flags.map((f, i) => (
                    <div key={i} style={{ background: '#fffbeb', border: '1px solid #fde68a', borderRadius: 8, padding: '10px 14px', fontSize: 12, color: '#92400e', display: 'flex', gap: 8 }}>
                      <span style={{ flexShrink: 0 }}>!</span>{f}
                    </div>
                  ))}
                </div>
              )}

              {/* Field scorecards */}
              <div style={{ display: 'grid', gap: 12, gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))' }}>
                {fields && fieldDefs.map(({ key, label, weight }) => {
                  const f = fields[key]
                  if (!f) return null
                  const imageQuality = fields?.imagesQuality
                  const mandatoryPhotos = imageQuality?.mandatoryPhotosPresent
                  const missingWorkmanshipCorePhotos = key === 'workmanship' && (!mandatoryPhotos?.workBefore || !mandatoryPhotos?.workAfter)
                  const synthesizedPaymentReviewReason =
                    key === 'paymentAttempted'
                    && !f.reviewReason
                    && f.value === 'No'
                    && (f.evidenceCited || []).some((item) => item.toLowerCase().includes('paymentstatus is null'))
                      ? 'No invoice or payment evidence is visible for this chargeable job, so payment needs manual review.'
                      : null
                  const effectiveReviewReason = f.reviewReason || synthesizedPaymentReviewReason
                  const missingTimeBenchmark = key === 'timeTaken' && ((f.expectedMinutes ?? null) === null || f.benchmarkSource === 'none')
                  const fieldForDisplay = missingWorkmanshipCorePhotos
                    ? {
                        ...f,
                        score: 2,
                        outcome: 'Fail',
                        salesforceValue: 'Urgent Issue',
                        rationale: 'Workmanship cannot be passed because the core before/after photo evidence is missing.',
                        reviewReason: null,
                        urgentIssueDescription: 'Missing work-before and/or work-after photos means workmanship cannot be validated properly.',
                      }
                    : effectiveReviewReason
                      ? { ...f, reviewReason: effectiveReviewReason }
                      : f
                  const displayOutcome = getDisplayOutcomeForField(fieldForDisplay, { forceReview: missingTimeBenchmark })
                  const oc = outcomeColor(displayOutcome)
                  const obg = outcomeBackground(displayOutcome)
                  const scoreBand = key === 'imagesQuality'
                    ? getImageQualityBandExplanation(fieldForDisplay.score)
                    : key === 'workmanship' || key === 'decisionMaking'
                      ? getScoreBandExplanation(fieldForDisplay.score)
                      : null
                  const missingWorkmanshipEvidence = key === 'workmanship' ? [
                    !mandatoryPhotos?.workBefore ? 'No clear work-before photo was available for the AI to judge the starting condition.' : null,
                    !mandatoryPhotos?.workAfter ? 'No clear work-after photo was available for the AI to judge the finished outcome.' : null,
                    (fieldForDisplay.reviewReason?.toLowerCase().includes('insufficient photo evidence') || fieldForDisplay.rationale?.toLowerCase().includes('photo evidence is limited')) ? 'The AI marked this for review because the available photo evidence was not strong enough to judge workmanship confidently.' : null,
                  ].filter(Boolean) as string[] : []
                  const decisionMakingEvidence = key === 'decisionMaking' ? [
                    appointment.Attendance_Report_for_Customer__c ? 'Attendance Report for Customer was used to assess diagnosis, actions taken, and customer-facing next steps.' : null,
                    appointment.Attendance_Notes_for_Office__c ? 'Attendance Notes for Office were used to assess office-facing context, escalations, and expectation management.' : null,
                    appointment.Description ? 'Job Description was used to compare the original problem with the diagnosis and approach reached on site.' : null,
                    f.reviewReason?.toLowerCase().includes('scope') ? 'The AI found a possible scope-change or customer-handling issue that needs human review.' : null,
                    f.reviewReason?.toLowerCase().includes('quote') || f.rationale?.toLowerCase().includes('quote') ? 'The AI considered whether further works or quotes should have been raised based on the engineer report.' : null,
                  ].filter(Boolean) as string[] : []
                  const imageQualityEvidence = key === 'imagesQuality' ? [
                    mandatoryPhotos?.location ? null : 'Mandatory location photo is missing.',
                    mandatoryPhotos?.workBefore ? null : 'Mandatory work-before photo is missing.',
                    mandatoryPhotos?.workAfter ? null : 'Mandatory work-after photo is missing.',
                    mandatoryPhotos?.jobCompletion ? null : 'Mandatory job-completion photo is missing.',
                    [mandatoryPhotos?.location, mandatoryPhotos?.workBefore, mandatoryPhotos?.workAfter, mandatoryPhotos?.jobCompletion].filter(Boolean).length < 4
                      ? `Only ${[mandatoryPhotos?.location, mandatoryPhotos?.workBefore, mandatoryPhotos?.workAfter, mandatoryPhotos?.jobCompletion].filter(Boolean).length} of 4 mandatory photos were detected, so the score should stay in the low band.`
                      : null,
                  ].filter(Boolean) as string[] : []
                  const reportEvidence = key === 'report' ? [
                    appointment.Description ? 'Job Description was used to assess whether the report explains the original problem clearly.' : null,
                    appointment.Attendance_Report_for_Customer__c ? 'Attendance Report for Customer was used to assess work summary, actions taken, and next steps.' : null,
                    f.reviewReason?.toLowerCase().includes('scope') ? 'The AI found a possible unresolved scope change or site issue in the written report.' : null,
                    f.reviewReason?.toLowerCase().includes('complaint') ? 'The AI found a flag or complaint in the notes that may require follow-up.' : null,
                  ].filter(Boolean) as string[] : []
                  const timeTakenEvidence = key === 'timeTaken' ? [
                    getStartTimeLabel(appointment) ? `Actual Start recorded as ${getStartTimeLabel(appointment)}.` : null,
                    appointment.ActualEndTimeFormatted ? `Actual End recorded as ${appointment.ActualEndTimeFormatted}.` : null,
                    f.actualMinutes !== undefined ? `Calculated duration is ${f.actualMinutes} minutes.` : null,
                    (f.expectedMinutes ?? null) !== null ? `Expected benchmark duration is ${f.expectedMinutes} minutes.` : 'No expected benchmark duration is available for this job, so Time on Site should stay in review.',
                    appointment.Scheduled_Trade__c ? `Scheduled Trade considered: ${appointment.Scheduled_Trade__c}.` : null,
                    appointment.Trade_Group_Region__c || appointment.Trade_Group_Postcode__c ? `Trade Group considered: ${appointment.Trade_Group_Region__c || appointment.Trade_Group_Postcode__c}.` : null,
                  ].filter(Boolean) as string[] : []
                  return (
                    <div key={key} data-driver={key === 'workmanship' ? 'scorecard-workmanship' : undefined} style={{ border: `1px solid ${colors.grayscale.border.default}`, borderRadius: 12, overflow: 'hidden', background: '#fff' }}>
                      <div style={{ background: obg, padding: '12px 16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <div>
                          <div style={{ fontSize: 12, fontWeight: 700, color: colors.primary.darker }}>{label}</div>
                          <div style={{ fontSize: 10, color: colors.text.grayscale.caption, marginTop: 1 }}>Weight: {weight}</div>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                          {f.score !== undefined && <span style={{ fontSize: 22, fontWeight: 800, color: oc }}>{f.score}</span>}
                          {f.value !== undefined && f.score === undefined && <span style={{ fontSize: 14, fontWeight: 700, color: oc }}>{f.value}</span>}
                            <span style={{ fontSize: 11, fontWeight: 700, color: oc, background: '#fff', border: `1px solid ${oc}`, padding: '2px 10px', borderRadius: 20 }}>{displayOutcome}</span>
                        </div>
                      </div>
                        <div style={{ padding: '12px 16px' }}>
                          {fieldForDisplay.salesforceValue && <div style={{ fontSize: 11, color: colors.text.grayscale.caption, marginBottom: 6, fontWeight: 600 }}>{fieldForDisplay.salesforceValue}</div>}
                          {(fieldForDisplay.rationale || synthesizedPaymentReviewReason) && <div style={{ fontSize: 12, color: colors.text.grayscale.body, lineHeight: 1.6 }}>{synthesizedPaymentReviewReason ? 'Charge total exists, but invoice/payment evidence is not visible in the job record.' : fieldForDisplay.rationale}</div>}
                          {effectiveReviewReason && <div style={{ fontSize: 11, color: '#d97706', marginTop: 8, padding: '6px 10px', background: '#fffbeb', borderRadius: 6 }}>Review required: {effectiveReviewReason}</div>}
                          {fieldForDisplay.urgentIssueDescription && <div style={{ fontSize: 11, color: '#dc2626', marginTop: 8, padding: '6px 10px', background: '#fef2f2', borderRadius: 6, fontWeight: 600 }}>! {fieldForDisplay.urgentIssueDescription}</div>}
                        {(f.missedOpportunities?.length ?? 0) > 0 && (
                          <div style={{ marginTop: 8 }}>{f.missedOpportunities!.map((m, i) => <div key={i} style={{ fontSize: 11, color: '#d97706', marginTop: 3 }}>- {m}</div>)}</div>
                        )}
                        {(f.evidenceCited?.length ?? 0) > 0 && (
                          <details style={{ marginTop: 10 }} data-driver={key === 'workmanship' ? 'score-evidence' : undefined}>
                            <summary style={{ fontSize: 11, color: colors.text.grayscale.caption, cursor: 'pointer', userSelect: 'none' }}>Evidence cited ({f.evidenceCited!.length})</summary>
                            <div style={{ marginTop: 6, paddingLeft: 8, borderLeft: `2px solid ${colors.grayscale.border.subtle}` }}>
                              {f.evidenceCited!.map((e, i) => <div key={i} style={{ fontSize: 11, color: colors.text.grayscale.subtle, marginTop: 3, lineHeight: 1.5 }}>{e}</div>)}
                            </div>
                          </details>
                        )}
                        {key === 'workmanship' && scoreGuideContent.workmanship && (
                          <details style={{ marginTop: 10 }} data-driver="workmanship-guide">
                            <summary style={{ fontSize: 11, color: colors.primary.darker, cursor: 'pointer', userSelect: 'none', fontWeight: 700 }}>Why this workmanship score was given</summary>
                            <div style={{ marginTop: 10, display: 'grid', gap: 10 }}>
                              {scoreBand && (
                                <div style={{ background: '#eff6ff', border: '1px solid #bfdbfe', borderRadius: 8, padding: '10px 12px' }}>
                                  <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 6 }}>Why this exact score</div>
                                  <div style={{ fontSize: 12, fontWeight: 700, color: colors.primary.darker }}>
                                    Score {fieldForDisplay.score} sits in the {scoreBand.band} band, which maps to {scoreBand.label} in Salesforce.
                                  </div>
                                  <div style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 6, lineHeight: 1.6 }}>
                                    {scoreBand.why}
                                  </div>
                                  {f.rationale && (
                                    <div style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 6, lineHeight: 1.6 }}>
                                        AI reasoning for this job: {fieldForDisplay.rationale}
                                    </div>
                                  )}
                                  {missingWorkmanshipEvidence.length > 0 && (
                                    <div style={{ marginTop: 8, padding: '8px 10px', background: '#fff7ed', border: '1px solid #fdba74', borderRadius: 8 }}>
                                      <div style={{ fontSize: 10, fontWeight: 700, color: '#9a3412', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>
                                        Evidence gap affecting this score
                                      </div>
                                      {missingWorkmanshipEvidence.map((item) => (
                                        <div key={item} style={{ fontSize: 11, color: '#9a3412', marginTop: 4, lineHeight: 1.5 }}>
                                          - {item}
                                        </div>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              )}
                              <div style={{ fontSize: 11, color: colors.text.grayscale.body, lineHeight: 1.6 }}>{scoreGuideContent.workmanship.purpose}</div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Evidence sources</div>
                                {scoreGuideContent.workmanship.evidence.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>- {item}</div>)}
                              </div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Scoring rubric</div>
                                {scoreGuideContent.workmanship.rubric.map((item) => <div key={item.band} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}><strong>{item.band}:</strong> {item.meaning}</div>)}
                              </div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Salesforce mapping</div>
                                {scoreGuideContent.workmanship.mapping.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>{item}</div>)}
                              </div>
                              {f.salesforceValue && (
                                <div style={{ background: '#f8fafc', borderRadius: 8, padding: '8px 10px', border: `1px solid ${colors.grayscale.border.subtle}` }}>
                                  <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>Salesforce result for this job</div>
                                  <div style={{ fontSize: 11, color: colors.text.grayscale.body }}>
                                      The AI returned a score of {fieldForDisplay.score}, so the Salesforce picklist value is <strong>{fieldForDisplay.salesforceValue}</strong>.
                                  </div>
                                </div>
                              )}
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>AI review triggers</div>
                                {scoreGuideContent.workmanship.triggers.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>- {item}</div>)}
                              </div>
                              <div style={{ fontSize: 11, color: '#0f766e', background: '#f0fdf4', borderRadius: 8, padding: '8px 10px' }}>{scoreGuideContent.workmanship.note}</div>
                            </div>
                          </details>
                        )}
                        {key === 'decisionMaking' && scoreGuideContent.decisionMaking && (
                          <details style={{ marginTop: 10 }}>
                            <summary style={{ fontSize: 11, color: colors.primary.darker, cursor: 'pointer', userSelect: 'none', fontWeight: 700 }}>Why this decision making score was given</summary>
                            <div style={{ marginTop: 10, display: 'grid', gap: 10 }}>
                              {scoreBand && (
                                <div style={{ background: '#eff6ff', border: '1px solid #bfdbfe', borderRadius: 8, padding: '10px 12px' }}>
                                  <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 6 }}>Why this exact score</div>
                                  <div style={{ fontSize: 12, fontWeight: 700, color: colors.primary.darker }}>
                                    Score {fieldForDisplay.score} sits in the {scoreBand.band} band, which maps to {scoreBand.label} in Salesforce.
                                  </div>
                                  <div style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 6, lineHeight: 1.6 }}>
                                    {scoreBand.why}
                                  </div>
                                  {f.rationale && (
                                    <div style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 6, lineHeight: 1.6 }}>
                                      AI reasoning for this job: {fieldForDisplay.rationale}
                                    </div>
                                  )}
                                  {decisionMakingEvidence.length > 0 && (
                                    <div style={{ marginTop: 8, padding: '8px 10px', background: '#fff7ed', border: '1px solid #fdba74', borderRadius: 8 }}>
                                      <div style={{ fontSize: 10, fontWeight: 700, color: '#9a3412', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>
                                        Engineer report evidence affecting this score
                                      </div>
                                      {decisionMakingEvidence.map((item) => (
                                        <div key={item} style={{ fontSize: 11, color: '#9a3412', marginTop: 4, lineHeight: 1.5 }}>
                                          - {item}
                                        </div>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              )}
                              <div style={{ fontSize: 11, color: colors.text.grayscale.body, lineHeight: 1.6 }}>{scoreGuideContent.decisionMaking.purpose}</div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Evidence sources</div>
                                {scoreGuideContent.decisionMaking.evidence.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>- {item}</div>)}
                              </div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Scoring rubric</div>
                                {scoreGuideContent.decisionMaking.rubric.map((item) => <div key={item.band} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}><strong>{item.band}:</strong> {item.meaning}</div>)}
                              </div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Salesforce mapping</div>
                                {scoreGuideContent.decisionMaking.mapping.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>{item}</div>)}
                              </div>
                              {f.salesforceValue && (
                                <div style={{ background: '#f8fafc', borderRadius: 8, padding: '8px 10px', border: `1px solid ${colors.grayscale.border.subtle}` }}>
                                  <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>Salesforce result for this job</div>
                                  <div style={{ fontSize: 11, color: colors.text.grayscale.body }}>
                                      The AI returned a score of {fieldForDisplay.score}, so the Salesforce picklist value is <strong>{fieldForDisplay.salesforceValue}</strong>.
                                  </div>
                                </div>
                              )}
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>AI review triggers</div>
                                {scoreGuideContent.decisionMaking.triggers.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>- {item}</div>)}
                              </div>
                              <div style={{ fontSize: 11, color: '#0f766e', background: '#f0fdf4', borderRadius: 8, padding: '8px 10px' }}>{scoreGuideContent.decisionMaking.note}</div>
                            </div>
                          </details>
                        )}
                        {key === 'imagesQuality' && scoreGuideContent.imagesQuality && (
                          <details style={{ marginTop: 10 }}>
                            <summary style={{ fontSize: 11, color: colors.primary.darker, cursor: 'pointer', userSelect: 'none', fontWeight: 700 }}>Why this image quality score was given</summary>
                            <div style={{ marginTop: 10, display: 'grid', gap: 10 }}>
                              {scoreBand && (
                                <div style={{ background: '#eff6ff', border: '1px solid #bfdbfe', borderRadius: 8, padding: '10px 12px' }}>
                                  <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 6 }}>Why this exact score</div>
                                  <div style={{ fontSize: 12, fontWeight: 700, color: colors.primary.darker }}>
                                    Score {fieldForDisplay.score} sits in the {scoreBand.band} band, which maps to {scoreBand.label} in Salesforce.
                                  </div>
                                  <div style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 6, lineHeight: 1.6 }}>
                                    {scoreBand.why}
                                  </div>
                                  {f.rationale && (
                                    <div style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 6, lineHeight: 1.6 }}>
                                      AI reasoning for this job: {fieldForDisplay.rationale}
                                    </div>
                                  )}
                                  {imageQualityEvidence.length > 0 && (
                                    <div style={{ marginTop: 8, padding: '8px 10px', background: '#fff7ed', border: '1px solid #fdba74', borderRadius: 8 }}>
                                      <div style={{ fontSize: 10, fontWeight: 700, color: '#9a3412', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>
                                        Mandatory photo validation affecting this score
                                      </div>
                                      {imageQualityEvidence.map((item) => (
                                        <div key={item} style={{ fontSize: 11, color: '#9a3412', marginTop: 4, lineHeight: 1.5 }}>
                                          - {item}
                                        </div>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              )}
                              <div style={{ fontSize: 11, color: colors.text.grayscale.body, lineHeight: 1.6 }}>{scoreGuideContent.imagesQuality.purpose}</div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Evidence sources</div>
                                {scoreGuideContent.imagesQuality.evidence.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>- {item}</div>)}
                              </div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Scoring rubric</div>
                                {scoreGuideContent.imagesQuality.rubric.map((item) => <div key={item.band} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}><strong>{item.band}:</strong> {item.meaning}</div>)}
                              </div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Salesforce mapping</div>
                                {scoreGuideContent.imagesQuality.mapping.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>{item}</div>)}
                              </div>
                              {f.salesforceValue && (
                                <div style={{ background: '#f8fafc', borderRadius: 8, padding: '8px 10px', border: `1px solid ${colors.grayscale.border.subtle}` }}>
                                  <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>Salesforce result for this job</div>
                                  <div style={{ fontSize: 11, color: colors.text.grayscale.body }}>
                                      The AI returned a score of {fieldForDisplay.score}, so the Salesforce picklist value is <strong>{fieldForDisplay.salesforceValue}</strong>.
                                  </div>
                                </div>
                              )}
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>AI review triggers</div>
                                {scoreGuideContent.imagesQuality.triggers.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>- {item}</div>)}
                              </div>
                              <div style={{ fontSize: 11, color: '#0f766e', background: '#f0fdf4', borderRadius: 8, padding: '8px 10px' }}>{scoreGuideContent.imagesQuality.note}</div>
                            </div>
                          </details>
                        )}
                        {key === 'report' && scoreGuideContent.report && (
                          <details style={{ marginTop: 10 }}>
                            <summary style={{ fontSize: 11, color: colors.primary.darker, cursor: 'pointer', userSelect: 'none', fontWeight: 700 }}>Why this report score was given</summary>
                            <div style={{ marginTop: 10, display: 'grid', gap: 10 }}>
                              {scoreBand && (
                                <div style={{ background: '#eff6ff', border: '1px solid #bfdbfe', borderRadius: 8, padding: '10px 12px' }}>
                                  <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 6 }}>Why this exact score</div>
                                  <div style={{ fontSize: 12, fontWeight: 700, color: colors.primary.darker }}>
                                    Score {fieldForDisplay.score} sits in the {scoreBand.band} band, which maps to {scoreBand.label} in Salesforce.
                                  </div>
                                  <div style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 6, lineHeight: 1.6 }}>
                                    {scoreBand.why}
                                  </div>
                                  {f.rationale && (
                                    <div style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 6, lineHeight: 1.6 }}>
                                      AI reasoning for this job: {fieldForDisplay.rationale}
                                    </div>
                                  )}
                                  {reportEvidence.length > 0 && (
                                    <div style={{ marginTop: 8, padding: '8px 10px', background: '#fff7ed', border: '1px solid #fdba74', borderRadius: 8 }}>
                                      <div style={{ fontSize: 10, fontWeight: 700, color: '#9a3412', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>
                                        Written report evidence affecting this score
                                      </div>
                                      {reportEvidence.map((item) => (
                                        <div key={item} style={{ fontSize: 11, color: '#9a3412', marginTop: 4, lineHeight: 1.5 }}>
                                          - {item}
                                        </div>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              )}
                              <div style={{ fontSize: 11, color: colors.text.grayscale.body, lineHeight: 1.6 }}>{scoreGuideContent.report.purpose}</div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Evidence sources</div>
                                {scoreGuideContent.report.evidence.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>- {item}</div>)}
                              </div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Scoring rubric</div>
                                {scoreGuideContent.report.rubric.map((item) => <div key={item.band} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}><strong>{item.band}:</strong> {item.meaning}</div>)}
                              </div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Salesforce mapping</div>
                                {scoreGuideContent.report.mapping.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>{item}</div>)}
                              </div>
                              {f.salesforceValue && (
                                <div style={{ background: '#f8fafc', borderRadius: 8, padding: '8px 10px', border: `1px solid ${colors.grayscale.border.subtle}` }}>
                                  <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>Salesforce result for this job</div>
                                  <div style={{ fontSize: 11, color: colors.text.grayscale.body }}>
                                      The AI returned a score of {fieldForDisplay.score}, so the Salesforce picklist value is <strong>{fieldForDisplay.salesforceValue}</strong>.
                                  </div>
                                </div>
                              )}
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>AI review triggers</div>
                                {scoreGuideContent.report.triggers.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>- {item}</div>)}
                              </div>
                              <div style={{ fontSize: 11, color: '#0f766e', background: '#f0fdf4', borderRadius: 8, padding: '8px 10px' }}>{scoreGuideContent.report.note}</div>
                            </div>
                          </details>
                        )}
                        {key === 'timeTaken' && scoreGuideContent.timeTaken && (
                          <details style={{ marginTop: 10 }}>
                            <summary style={{ fontSize: 11, color: colors.primary.darker, cursor: 'pointer', userSelect: 'none', fontWeight: 700 }}>Why this time on site score was given</summary>
                            <div style={{ marginTop: 10, display: 'grid', gap: 10 }}>
                              {scoreBand && (
                                <div style={{ background: '#eff6ff', border: '1px solid #bfdbfe', borderRadius: 8, padding: '10px 12px' }}>
                                  <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 6 }}>Why this exact score</div>
                                  <div style={{ fontSize: 12, fontWeight: 700, color: colors.primary.darker }}>
                                    Score {fieldForDisplay.score} sits in the {scoreBand.band} band.
                                  </div>
                                  <div style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 6, lineHeight: 1.6 }}>
                                    {missingTimeBenchmark
                                      ? 'A benchmark is missing for this trade and job type, so Time on Site should be treated as review-led context rather than a confident pass/fail judgment.'
                                      : scoreBand.why}
                                  </div>
                                  {f.rationale && (
                                    <div style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 6, lineHeight: 1.6 }}>
                                      AI reasoning for this job: {fieldForDisplay.rationale}
                                    </div>
                                  )}
                                  {timeTakenEvidence.length > 0 && (
                                    <div style={{ marginTop: 8, padding: '8px 10px', background: '#fff7ed', border: '1px solid #fdba74', borderRadius: 8 }}>
                                      <div style={{ fontSize: 10, fontWeight: 700, color: '#9a3412', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>
                                        Timing evidence affecting this score
                                      </div>
                                      {timeTakenEvidence.map((item) => (
                                        <div key={item} style={{ fontSize: 11, color: '#9a3412', marginTop: 4, lineHeight: 1.5 }}>
                                          - {item}
                                        </div>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              )}
                              <div style={{ fontSize: 11, color: colors.text.grayscale.body, lineHeight: 1.6 }}>{scoreGuideContent.timeTaken.purpose}</div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Evidence sources</div>
                                {scoreGuideContent.timeTaken.evidence.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>- {item}</div>)}
                              </div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Scoring rubric</div>
                                {scoreGuideContent.timeTaken.rubric.map((item) => <div key={item.band} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}><strong>{item.band}:</strong> {item.meaning}</div>)}
                              </div>
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Salesforce mapping</div>
                                {scoreGuideContent.timeTaken.mapping.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>{item}</div>)}
                              </div>
                              {f.salesforceValue && (
                                <div style={{ background: '#f8fafc', borderRadius: 8, padding: '8px 10px', border: `1px solid ${colors.grayscale.border.subtle}` }}>
                                  <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>Salesforce result for this job</div>
                                  <div style={{ fontSize: 11, color: colors.text.grayscale.body }}>
                                    {missingTimeBenchmark
                                      ? 'A benchmark is missing, so this field should be treated as a review-led timing assessment even if a provisional score was returned.'
                                      : <>The AI returned a score of {fieldForDisplay.score}, so the Salesforce timing value is <strong>{fieldForDisplay.salesforceValue}</strong>.</>}
                                  </div>
                                </div>
                              )}
                              <div>
                                <div style={{ fontSize: 10, fontWeight: 700, color: colors.text.grayscale.caption, textTransform: 'uppercase', letterSpacing: '0.08em' }}>AI review triggers</div>
                                {scoreGuideContent.timeTaken.triggers.map((item) => <div key={item} style={{ fontSize: 11, color: colors.text.grayscale.body, marginTop: 4 }}>- {item}</div>)}
                              </div>
                              <div style={{ fontSize: 11, color: '#0f766e', background: '#f0fdf4', borderRadius: 8, padding: '8px 10px' }}>{scoreGuideContent.timeTaken.note}</div>
                            </div>
                          </details>
                        )}
                      </div>
                    </div>
                  )
                })}
                {/* 8th card — Overall Result + Salesforce TQR Check fields */}
                {(() => {
                  const f = tqrResult.tqr_fields
                  const imgSf = (f?.imagesQuality?.salesforceValue || '').toLowerCase()
                  const imagesQualityOut = ['perfect', 'good', 'acceptable'].includes(imgSf) ? 'Good'
                    : ['non acceptable', 'urgent issue', 'poor'].includes(imgSf) ? 'Poor'
                    : imgSf ? 'N/A' : '—'
                  const _sig = f?.customerSignature
                  const _sigOutcome = (_sig?.outcome || '').toLowerCase()
                  const _sigValue = (_sig?.value || _sig?.salesforceValue || '').toLowerCase()
                  const signedOut = (_sigOutcome === 'pass' || _sigValue === 'yes' || _sigValue === 'na_customernotpresent') ? 'Yes' : 'No'
                  const payVal = f?.paymentAttempted?.value || ''
                  const payOut = payVal === 'CreditAccount' ? 'Credit Account' : payVal || '—'

                  const sfRows: { label: string; value: string | undefined }[] = [
                    { label: 'Post Visit Report Check',   value: tqrResult.verdict },
                    { label: 'Images Quality',            value: imagesQualityOut },
                    { label: 'Did the Customer Sign SR?', value: signedOut },
                    { label: 'Time Taken',                value: f?.timeTaken?.salesforceValue },
                    { label: 'Workmanship',               value: f?.workmanship?.salesforceValue },
                    { label: 'DecisionMaking',            value: f?.decisionMaking?.salesforceValue },
                    { label: 'Report',                    value: f?.report?.salesforceValue },
                    { label: 'Payment Attempted',         value: payOut },
                  ]

                  const sfColor = (val: string | undefined) => {
                    if (!val) return colors.text.grayscale.caption
                    const v = val.toLowerCase()
                    if (['tqr', 'perfect', 'good', 'yes', 'ideal'].some(k => v.includes(k))) return '#16a34a'
                    if (['unacceptable', 'urgent issue', 'non acceptable', 'poor', 'excessive', 'rushed'].some(k => v.includes(k))) return '#dc2626'
                    if (['sub standard', 'acceptable', 'n/a', 'credit account', 'no'].some(k => v.includes(k))) return '#d97706'
                    return colors.text.grayscale.body
                  }

                  return (
                    <div style={{ border: `2px solid ${verdictBg}`, borderRadius: 12, overflow: 'hidden', background: '#fff' }}>
                      {/* Header */}
                      <div style={{ background: verdictBg, padding: '12px 16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <div>
                          <div style={{ fontSize: 12, fontWeight: 700, color: '#fff' }}>Overall Result</div>
                          <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.75)', marginTop: 1 }}>Final TQR Score</div>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                          <span style={{ fontSize: 22, fontWeight: 800, color: '#fff' }}>{tqrResult.overall}</span>
                          <span style={{ fontSize: 11, fontWeight: 700, color: verdictBg, background: '#fff', padding: '2px 10px', borderRadius: 20 }}>{tqrResult.verdict}</span>
                        </div>
                      </div>
                      {/* Salesforce TQR Check fields */}
                      <div style={{ padding: '8px 0' }}>
                        {sfRows.map(({ label, value }) => (
                          <div key={label} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '6px 14px', borderBottom: `1px solid ${colors.grayscale.border.subtle}` }}>
                            <div style={{ fontSize: 11, color: colors.text.grayscale.caption, fontWeight: 500 }}>{label}</div>
                            <div style={{ fontSize: 12, fontWeight: 800, color: sfColor(value) }}>{value || '—'}</div>
                          </div>
                        ))}
                        {tqrResult.hard_fail && (
                          <div style={{ margin: '8px 12px 4px', background: '#fef2f2', border: '1px solid #fecaca', color: '#dc2626', fontSize: 11, fontWeight: 700, padding: '6px 10px', borderRadius: 6 }}>
                            ⛔ Hard Fail Triggered
                          </div>
                        )}
                      </div>
                    </div>
                  )
                })()}
              </div>
            </div>
          )
        })()}
      </Section>

      {/* Job Photos */}
      <Section title={`Job Photos${workOrderImages.length > 0 ? ` - ${workOrderImages.length}` : ''}`} accent="#6d28d9">
        {imagesLoading && <div style={{ fontSize: 13, color: colors.text.grayscale.subtle, padding: '16px 0' }}>Loading photos...</div>}
        {!imagesLoading && imagesError && <div style={{ background: '#fef2f2', borderRadius: 8, padding: '12px 16px', fontSize: 13, color: '#dc2626' }}>{imagesError}</div>}
        {!imagesLoading && !imagesError && workOrderImages.length === 0 && <div style={{ fontSize: 13, color: colors.text.grayscale.subtle, padding: '16px 0' }}>No photos found for this work order.</div>}
        <div style={{ display: 'grid', gap: 10, gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))' }}>
          {workOrderImages.map((image) => (
            <div key={image.id} style={{ borderRadius: 10, overflow: 'hidden', cursor: 'pointer', border: `1px solid ${colors.grayscale.border.subtle}`, background: '#fff' }} onClick={() => setSelectedImage(image)}>
              <img src={`data:${image.contentType};base64,${image.base64}`} alt={image.title} style={{ width: '100%', height: 120, objectFit: 'cover', display: 'block' }} />
              <div style={{ padding: '6px 8px', fontSize: 10, color: colors.text.grayscale.caption, fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', background: '#fff' }}>{image.title}</div>
            </div>
          ))}
        </div>
      </Section>

      {selectedImage && (
        <div
          onClick={() => setSelectedImage(null)}
          style={{
            position: 'fixed',
            inset: 0,
            background: 'rgba(15, 23, 42, 0.72)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            padding: 24,
            zIndex: 1000,
          }}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            style={{
              width: 'min(1200px, 96vw)',
              maxHeight: '92vh',
              overflow: 'auto',
              background: '#fff',
              borderRadius: 18,
              boxShadow: '0 24px 80px rgba(15,23,42,0.28)',
              display: 'grid',
              gridTemplateColumns: 'minmax(0, 1.3fr) minmax(320px, 0.7fr)',
              gap: 0,
            }}
          >
            <div style={{ background: '#0f172a', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 20 }}>
              <img
                src={`data:${selectedImage.contentType};base64,${selectedImage.base64}`}
                alt={selectedImage.title}
                style={{ maxWidth: '100%', maxHeight: '78vh', objectFit: 'contain', borderRadius: 12, background: '#fff' }}
              />
            </div>
            <div style={{ padding: 24, display: 'flex', flexDirection: 'column', gap: 16 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 12 }}>
                <div>
                  <div style={{ fontSize: 11, fontWeight: 800, letterSpacing: '0.14em', textTransform: 'uppercase', color: colors.primary.default }}>AI Image Review</div>
                  <div style={{ marginTop: 8, fontSize: 18, fontWeight: 800, color: colors.grayscale.title, lineHeight: 1.3 }}>{selectedImage.title}</div>
                </div>
                <button
                  onClick={() => setSelectedImage(null)}
                  style={{ border: `1px solid ${colors.grayscale.border.default}`, background: '#fff', borderRadius: 10, width: 36, height: 36, cursor: 'pointer', color: colors.text.grayscale.body, fontSize: 18 }}
                >
                  x
                </button>
              </div>
              <div style={{ padding: '14px 16px', borderRadius: 12, background: colors.primary.subtle, border: `1px solid ${colors.border.primary.subtle}` }}>
                <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: colors.primary.darker, marginBottom: 8 }}>AI Description</div>
                <div style={{ fontSize: 13, lineHeight: 1.75, color: colors.text.grayscale.body }}>
                  {getImageDescription(selectedImage) || 'Run or re-run the AI analysis for this job to generate a detailed description for this image.'}
                </div>
              </div>
              <div style={{ padding: '14px 16px', borderRadius: 12, background: '#f8fafc', border: `1px solid ${colors.grayscale.border.subtle}` }}>
                <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: colors.text.grayscale.caption, marginBottom: 8 }}>Image File</div>
                <div style={{ fontSize: 13, color: colors.text.grayscale.body, lineHeight: 1.6 }}>{selectedImage.title}</div>
              </div>
            </div>
          </div>
        </div>
      )}

    </div>
  )
}

export default function Dashboard() {
  const [view, setView] = useState<View>('dashboard')
  const [stats, setStats] = useState<DashboardStats | null>(null)
  const [appointments, setAppointments] = useState<Appointment[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [totalPages, setTotalPages] = useState(1)
  const [loading, setLoading] = useState(false)
  const [filters, setFilters] = useState({ search: '', engineer: '', trade: '', sector: '' })
  const [selectedAppointment, setSelectedAppointment] = useState<Appointment | null>(null)
  const [workOrderImages, setWorkOrderImages] = useState<WorkOrderImage[]>([])
  const [imagesLoading, setImagesLoading] = useState(false)
  const [imagesError, setImagesError] = useState<string | null>(null)
  const [tqrResult, setTqrResult] = useState<TqrResult | null>(null)
  const [analyseLoading, setAnalyseLoading] = useState(false)
  const [analyseError, setAnalyseError] = useState<string | null>(null)

  const loadStats = async () => {
    const { data } = await api.get('/api/dashboard/stats')
    setStats(data)
  }

  const loadAppointments = async () => {
    setLoading(true)
    try {
      const { data } = await api.get('/api/appointments', {
        params: {
          page,
          pageSize: 25,
          search: filters.search || undefined,
          engineer: filters.engineer || undefined,
          trade: filters.trade || undefined,
          sector: filters.sector || undefined,
        },
      })
      setAppointments(data.records || [])
      setTotal(data.total || 0)
      setTotalPages(data.totalPages || 1)
    } finally {
      setLoading(false)
    }
  }

  const openAppointment = async (id: string) => {
    setWorkOrderImages([])
    setImagesError(null)
    setTqrResult(null)
    setAnalyseError(null)
    const { data } = await api.get(`/api/appointments/${id}`)
    setSelectedAppointment(data)
    if (data.tqrResult) setTqrResult(data.tqrResult)
    setView('appointment-detail')
  }

  const runAnalysis = async (appointmentId: string, force = false) => {
    setAnalyseLoading(true)
    setAnalyseError(null)
    try {
      const { data } = await api.post(`/api/analyse/${appointmentId}`, null, {
        params: force ? { force: true } : undefined,
      })
      setTqrResult(data)
    } catch (e: unknown) {
      const msg = (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail
      setAnalyseError(msg || 'Analysis failed. Please try again.')
    } finally {
      setAnalyseLoading(false)
    }
  }

  const downloadReport = () => {
    if (!selectedAppointment) return

    const appointment = selectedAppointment
    const reportWindow = window.open('', '_blank', 'width=1200,height=900')
    if (!reportWindow) return

    const workOrderLabel = appointment.workOrder?.WorkOrderNumber || appointment.Work_Order__c || ''
    const address = [appointment.Street, appointment.City, appointment.PostalCode].filter(Boolean).join(', ') || ''
    const tqrFields = tqrResult?.tqr_fields
    const tqrCards = [
      { label: 'Workmanship', weight: '20%', field: tqrFields?.workmanship, fallback: appointment.Workmanship__c || '' },
      { label: 'Decision Making', weight: '20%', field: tqrFields?.decisionMaking, fallback: appointment.Decision_Making__c || '' },
      { label: 'Image Quality', weight: '20%', field: tqrFields?.imagesQuality, fallback: '' },
      { label: 'Report', weight: '15%', field: tqrFields?.report, fallback: appointment.Report__c || '' },
      { label: 'Time on Site', weight: '10%', field: tqrFields?.timeTaken, fallback: appointment.Duration ? `${appointment.Duration} mins` : '' },
      { label: 'Payment', weight: '10%', field: tqrFields?.paymentAttempted, fallback: appointment.Payment_Attempted__c || '' },
      { label: 'Signature', weight: '5%', field: tqrFields?.customerSignature, fallback: '' },
    ]

      const fieldHtml = tqrCards.map(({ label, weight, field, fallback }) => {
        const displayOutcome = field ? getDisplayOutcomeForField(field) : 'Pending'
        return `
        <div class="score-card">
          <div class="score-head">
            <div>
              <div class="score-title">${escapeHtml(label)}</div>
              <div class="score-weight">Weight: ${escapeHtml(weight)}</div>
          </div>
          <div class="score-value">${escapeHtml(field?.score ?? field?.value ?? fallback)}</div>
        </div>
          <div class="score-meta">${escapeHtml(displayOutcome)}${field?.salesforceValue ? ` - ${escapeHtml(field.salesforceValue)}` : ''}</div>
          <div class="score-text">${escapeHtml(field?.rationale || '')}</div>
        </div>
      `}).join('')

    const imagesHtml = workOrderImages.length > 0
      ? workOrderImages.map((image) => `
          <div class="photo-card">
            <img src="data:${image.contentType};base64,${image.base64}" alt="${escapeHtml(image.title)}" />
            <div class="photo-title">${escapeHtml(image.title)}</div>
          </div>
        `).join('')
      : '<div class="empty">No job photos available.</div>'
    const documentsHtml = (appointment.documents || []).length > 0
      ? (appointment.documents || []).map((doc) => `
          <div style="padding:12px 14px;border:1px solid #e9d5ff;border-radius:10px;background:#faf5ff">
            <div style="font-size:13px;font-weight:700;color:#1A1D23">${escapeHtml(doc.title)}</div>
            <div style="font-size:11px;color:#646F86;margin-top:4px;text-transform:capitalize">${escapeHtml(doc.category || 'document')}${doc.fileType ? ` - ${escapeHtml(doc.fileType)}` : ''}${doc.source ? ` - ${escapeHtml(doc.source)}` : ''}</div>
          </div>
        `).join('')
      : '<div class="empty">No invoice, payment, or service report documents found.</div>'

    const html = `
      <!doctype html>
      <html>
      <head>
        <meta charset="utf-8" />
        <title>${escapeHtml(appointment.AppointmentNumber || 'Engineer Job Report')}</title>
        <style>
          body { font-family: Montserrat, Arial, sans-serif; margin: 0; color: #1A1D23; background: #fff; }
          .page { padding: 32px; }
          .hero { background: linear-gradient(135deg, #17325E 0%, #27549D 100%); color: #fff; border-radius: 16px; padding: 24px 28px; }
          .hero-grid, .grid, .scores, .photos { display: grid; gap: 18px; }
          .hero-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); margin-top: 18px; }
          .grid { grid-template-columns: repeat(4, minmax(0, 1fr)); }
          .scores { grid-template-columns: repeat(2, minmax(0, 1fr)); }
          .photos { grid-template-columns: repeat(3, minmax(0, 1fr)); }
          .section { margin-top: 24px; border: 1px solid #E8EAEE; border-radius: 16px; padding: 22px 24px; }
          .label { font-size: 10px; font-weight: 700; letter-spacing: 0.12em; text-transform: uppercase; color: #848EA3; margin-bottom: 6px; }
          .value { font-size: 13px; line-height: 1.7; }
          .section-title { font-size: 12px; font-weight: 800; letter-spacing: 0.16em; text-transform: uppercase; color: #17325E; margin-bottom: 18px; }
          .summary { background: #F7F9FD; border-left: 3px solid #7099DB; padding: 14px 18px; border-radius: 0 10px 10px 0; line-height: 1.7; font-size: 13px; }
          .score-card { border: 1px solid #E8EAEE; border-radius: 12px; padding: 14px 16px; }
          .score-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
          .score-title { font-size: 12px; font-weight: 700; color: #17325E; }
          .score-weight, .score-meta, .photo-title, .empty { font-size: 11px; color: #646F86; }
          .score-value { font-size: 22px; font-weight: 800; color: #17325E; }
          .score-text { font-size: 12px; line-height: 1.6; margin-top: 8px; }
          .photo-card img { width: 100%; height: 180px; object-fit: cover; border-radius: 10px; display: block; }
          .photo-title { margin-top: 8px; font-weight: 600; }
          @media print {
            body { background: #fff; }
            .page { padding: 20px; }
            .section, .hero { break-inside: avoid; }
          }
        </style>
      </head>
      <body>
        <div class="page">
          <div class="hero">
            <div style="font-size:11px;font-weight:700;letter-spacing:0.16em;text-transform:uppercase;color:rgba(255,255,255,0.72)">Engineer Job Report</div>
            <div style="font-size:28px;font-weight:800;margin-top:8px">${escapeHtml(appointment.AppointmentNumber || '')}</div>
            <div class="hero-grid">
              <div><div class="label" style="color:rgba(255,255,255,0.65)">Customer</div><div class="value">${escapeHtml(appointment.account?.Name)}</div></div>
              <div><div class="label" style="color:rgba(255,255,255,0.65)">Engineer</div><div class="value">${escapeHtml(appointment.AllocatedEngineerName || '')}</div></div>
              <div><div class="label" style="color:rgba(255,255,255,0.65)">Work Type</div><div class="value">${escapeHtml(appointment.workOrder?.WorkType?.Name || appointment.Scheduled_Trade__c)}</div></div>
              <div><div class="label" style="color:rgba(255,255,255,0.65)">Actual End</div><div class="value">${escapeHtml(appointment.ActualEndTimeFormatted)}</div></div>
              <div><div class="label" style="color:rgba(255,255,255,0.65)">Work Order</div><div class="value">${escapeHtml(workOrderLabel)}</div></div>
              <div><div class="label" style="color:rgba(255,255,255,0.65)">Duration</div><div class="value">${escapeHtml(appointment.Duration ? `${appointment.Duration} mins` : '')}</div></div>
            </div>
          </div>

          <div class="section">
            <div class="section-title">Customer Detail</div>
            <div class="grid">
              <div><div class="label">Account</div><div class="value">${escapeHtml(appointment.account?.Name)}</div></div>
              <div><div class="label">Account Manager</div><div class="value">${escapeHtml(appointment.accountManager)}</div></div>
              <div><div class="label">Site</div><div class="value">${escapeHtml(appointment.site)}</div></div>
              <div><div class="label">Address</div><div class="value">${escapeHtml(address)}</div></div>
              <div><div class="label">Phone</div><div class="value">${escapeHtml(appointment.account?.Phone)}</div></div>
              <div><div class="label">Email</div><div class="value"></div></div>
            </div>
          </div>

          <div class="section">
            <div class="section-title">Booking Detail</div>
            <div class="grid">
              <div><div class="label">Work Order</div><div class="value">${escapeHtml(workOrderLabel)}</div></div>
              <div><div class="label">Work Type</div><div class="value">${escapeHtml(appointment.workOrder?.WorkType?.Name)}</div></div>
              <div><div class="label">Scheduled Trade</div><div class="value">${escapeHtml(appointment.Scheduled_Trade__c)}</div></div>
              <div><div class="label">Trade Group</div><div class="value">${escapeHtml(getTradeGroupLabel(appointment))}</div></div>
              <div><div class="label">Sector Type</div><div class="value">${escapeHtml(appointment.Sector_Type__c)}</div></div>
              <div><div class="label">Allocated Engineer</div><div class="value">${escapeHtml(appointment.AllocatedEngineerName || '')}</div></div>
              <div><div class="label">Scheduled Start</div><div class="value">${escapeHtml(appointment.SchedStartTimeFormatted)}</div></div>
              <div><div class="label">Arrival Window</div><div class="value">${escapeHtml(appointment.ArrivalWindowStartTimeFormatted)}</div></div>
              <div><div class="label">Actual End</div><div class="value">${escapeHtml(appointment.ActualEndTimeFormatted)}</div></div>
              <div><div class="label">Duration</div><div class="value">${escapeHtml(appointment.Duration ? `${appointment.Duration} mins` : '')}</div></div>
            </div>
            <div style="margin-top:18px">
              <div class="label">Scope of Works</div>
              <div class="value">${escapeHtml(appointment.workOrder?.Description || appointment.Description)}</div>
            </div>
          </div>

          <div class="section">
            <div class="section-title">Engineer Job Report</div>
            <div class="grid" style="grid-template-columns:repeat(2,minmax(0,1fr));margin-bottom:16px">
              <div><div class="label">Actual Start</div><div class="value">${escapeHtml(getStartTimeLabel(appointment) || '')}</div></div>
              <div><div class="label">Actual End</div><div class="value">${escapeHtml(appointment.ActualEndTimeFormatted || '')}</div></div>
            </div>
            <div class="grid" style="grid-template-columns:repeat(3,minmax(0,1fr))">
              <div><div class="label">Attendance Notes for Office</div><div class="value">${escapeHtml(appointment.Attendance_Notes_for_Office__c || '')}</div></div>
              <div><div class="label">Attendance Report</div><div class="value">${escapeHtml(appointment.Attendance_Report_for_Customer__c)}</div></div>
              <div><div class="label">Feedback Notes</div><div class="value">${escapeHtml(appointment.Feedback_Notes__c)}</div></div>
              <div><div class="label">Description</div><div class="value">${escapeHtml(appointment.Description)}</div></div>
            </div>
          </div>

          <div class="section">
            <div class="section-title">Related Documents</div>
            <div class="grid" style="grid-template-columns:1fr">${documentsHtml}</div>
          </div>

          <div class="section">
            <div class="section-title">TQR AI Analysis</div>
            ${tqrResult ? `
              <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:18px">
                <div style="background:${escapeHtml(tqrResult.verdict === 'TQR' ? '#16a34a' : tqrResult.verdict === 'Sub standard' ? '#d97706' : '#dc2626')};color:#fff;font-weight:800;font-size:13px;padding:6px 16px;border-radius:8px">${escapeHtml(tqrResult.verdict || 'Pending')}</div>
                <div style="font-size:30px;font-weight:800;color:#17325E">${escapeHtml(tqrResult.overall)}/10</div>
                ${tqrResult.hard_fail ? '<div style="background:#fef2f2;border:1px solid #fecaca;color:#dc2626;font-size:11px;font-weight:700;padding:4px 12px;border-radius:6px">Hard Fail Triggered</div>' : ''}
                <div style="margin-left:auto;font-size:11px;color:#848EA3">${escapeHtml(tqrResult.analysed_at)}</div>
              </div>
              <div class="summary">${escapeHtml(tqrResult.summary)}</div>
              <div class="scores" style="margin-top:18px">${fieldHtml}</div>
            ` : '<div class="empty">No AI analysis has been run for this job yet.</div>'}
          </div>

          <div class="section">
            <div class="section-title">Job Photos - ${workOrderImages.length}</div>
            <div class="photos">${imagesHtml}</div>
          </div>
        </div>
      </body>
      </html>
    `

    reportWindow.document.open()
    reportWindow.document.write(html)
    reportWindow.document.close()
    reportWindow.focus()
    reportWindow.print()
  }

  const openWorkOrder = async (workOrderId: string | null | undefined) => {
    if (!workOrderId) return
    setImagesLoading(true)
    setImagesError(null)
    setWorkOrderImages([])
    if (view !== 'appointment-detail') setView('appointment-detail')
    try {
      const { data } = await api.get(`/api/work-orders/${workOrderId}/images`)
      setWorkOrderImages(data.images || [])
    } catch {
      setImagesError('Failed to load images for this work order.')
    } finally {
      setImagesLoading(false)
    }
  }

  useEffect(() => {
    void loadStats()
  }, [])

  useEffect(() => {
    void loadAppointments()
  }, [page, filters.search, filters.engineer, filters.trade, filters.sector])

  const heroTitle = useMemo(() => {
    if (view === 'dashboard') return 'Chumley TQR AI Review - last 90 days'
    if (view === 'appointments') return "not checked by TM's"
    return selectedAppointment?.AppointmentNumber || 'Appointment Detail'
  }, [view, selectedAppointment])

  return (
    <div style={pageStyle}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap');
        * { box-sizing: border-box; }
        @keyframes spin { to { transform: rotate(360deg); } }
        body { margin: 0; background: #F5F7FA; font-family: 'Montserrat', 'Segoe UI', sans-serif; }
        button, input, select { font-family: 'Montserrat', 'Segoe UI', sans-serif; }
      `}</style>

      <div style={{ maxWidth: 1680, margin: '0 auto', display: 'flex', minHeight: '100vh', flexWrap: 'wrap' }}>
        <aside style={{ width: 216, background: '#fff', borderRight: `1px solid ${colors.grayscale.border.subtle}`, padding: 14, boxShadow: '0 8px 30px rgba(23,50,94,0.06)' }}>
          <div style={{ ...cardStyle, padding: '14px 14px 10px', borderRadius: 18 }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 14 }}>
              <img src="/aspectLogo.svg" alt="Aspect logo" style={{ height: 34, width: 'auto', display: 'block' }} />
            </div>
          </div>

          <div style={{ marginTop: 14, display: 'grid', gap: 4 }}>
            <button
              style={{ width: '100%', padding: '10px 14px', fontSize: 13, fontWeight: 700, border: 'none', borderRadius: 10, cursor: 'pointer', textAlign: 'left', background: view === 'dashboard' ? colors.primary.darker : 'transparent', color: view === 'dashboard' ? '#fff' : colors.text.grayscale.body, fontFamily: 'inherit' }}
              onClick={() => setView('dashboard')}
            >Dashboard</button>
            <button
              style={{ width: '100%', padding: '10px 14px', fontSize: 13, fontWeight: 700, border: 'none', borderRadius: 10, cursor: 'pointer', textAlign: 'left', background: view === 'appointments' ? colors.primary.darker : 'transparent', color: view === 'appointments' ? '#fff' : colors.text.grayscale.body, fontFamily: 'inherit' }}
              onClick={() => setView('appointments')}
            >Appointments</button>
          </div>
        </aside>

        <main style={{ flex: 1, minWidth: 0, padding: 16 }}>
          {view !== 'appointments' && (
            <div style={{ borderRadius: 16, backgroundImage: 'url(/London_Skyliner.png)', backgroundSize: 'cover', backgroundPosition: 'center 60%', height: 90, marginBottom: 16, position: 'relative', overflow: 'hidden' }}>
              <div style={{ position: 'absolute', inset: 0, background: 'linear-gradient(90deg, rgba(17,38,74,0.88) 0%, rgba(17,38,74,0.55) 100%)' }} />
              <div style={{ position: 'relative', zIndex: 1, height: '100%', display: 'flex', flexDirection: 'column', justifyContent: 'center', padding: '0 24px' }}>
                <p style={{ margin: 0, color: 'rgba(255,255,255,0.65)', fontSize: 9, fontWeight: 700, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Chumley AI-Powered Quality Review</p>
                <div style={{ marginTop: 4, fontSize: 20, fontWeight: 800, color: '#fff', lineHeight: 1.2 }}>{heroTitle}</div>
              </div>
            </div>
          )}

          {view === 'dashboard' && <DashboardView stats={stats} />}
          {view === 'appointments' && (
            <AppointmentsView
              appointments={appointments}
              total={total}
              page={page}
              totalPages={totalPages}
              loading={loading}
              filters={filters}
              setFilters={setFilters}
              setPage={setPage}
              onOpenAppointment={openAppointment}
              onOpenWorkOrder={openWorkOrder}
            />
          )}
          {view === 'appointment-detail' && (
            <AppointmentDetailView
              appointment={selectedAppointment}
              workOrderImages={workOrderImages}
              imagesLoading={imagesLoading}
              imagesError={imagesError}
              tqrResult={tqrResult}
              analyseLoading={analyseLoading}
              analyseError={analyseError}
              onBack={() => setView('appointments')}
              onOpenWorkOrder={openWorkOrder}
              onRunAnalysis={(id) => runAnalysis(id, false)}
              onReanalyse={(id) => runAnalysis(id, true)}
              onDownloadReport={downloadReport}
            />
          )}
        </main>
      </div>
    </div>
  )
}
