library(data.table)
library(arrow)
library(ggplot2)
library(stringr)
library(scales)

if (!dir.exists("output")) dir.create("output")


step_time <- function(label, code) {
  cat(sprintf("\n>>> %s ...\n", label))
  t0 <- proc.time(); result <- code
  cat(sprintf("    Done in %.1f sec\n", (proc.time() - t0)[["elapsed"]]))
  result
}

# =============================================================================
# 1. LOAD DATA
# =============================================================================
cat("\n== 1. LOAD DATA ==\n\n")
dt <- step_time("Reading parquet files", {
  rbindlist(list(
    as.data.table(read_parquet("data/train.parquet")),
    as.data.table(read_parquet("data/validation.parquet")),
    as.data.table(read_parquet("data/test.parquet"))
  ), use.names = TRUE, fill = TRUE)
})
cat(sprintf("\n  Rows    : %s\n  Columns : %s\n  Columns : %s\n",
            format(nrow(dt),big.mark=","), ncol(dt), paste(names(dt),collapse=", ")))
print(head(dt, 3))

# =============================================================================
# 2. DATA CLEANING
# =============================================================================
cat("\n== 2. DATA CLEANING ==\n\n")
dt <- step_time("Removing unknown labels", {
  before <- nrow(dt); dt <- dt[tolower(matched_label) != "unknown"]
  cat(sprintf("    Dropped (unknown): %s\n", format(before-nrow(dt),big.mark=","))); dt
})
dt <- step_time("Removing blank/NA labels", {
  before <- nrow(dt)
  dt <- dt[!is.na(matched_label) & nchar(trimws(matched_label)) > 0]
  cat(sprintf("    Dropped (blank)  : %s\n", format(before-nrow(dt),big.mark=","))); dt
})
dt <- step_time("Removing duplicates per user per year", {
  before <- nrow(dt); dt <- unique(dt, by=c("person_id","matched_label","start_date"))
  cat(sprintf("    Dropped (dupes)  : %s\n", format(before-nrow(dt),big.mark=","))); dt
})
dt <- step_time("Extracting year from start_date", {
  dt[, year := as.integer(str_extract(start_date, "\\d{4}"))]
  before <- nrow(dt); dt <- dt[!is.na(year) & year >= 1950 & year <= 2024]
  cat(sprintf("    Dropped (year)   : %s\n", format(before-nrow(dt),big.mark=",")))
  setorder(dt, person_id, year); dt
})
cat(sprintf("\n  Clean rows : %s | Users : %s | Jobs : %s | Years : %s-%s\n",
            format(nrow(dt),big.mark=","), format(uniqueN(dt$person_id),big.mark=","),
            format(uniqueN(dt$matched_label),big.mark=","), min(dt$year), max(dt$year)))

# =============================================================================
# 3. SECTOR MAPPING
# =============================================================================
cat("\n== 3. SECTOR MAPPING ==\n\n")

map_sectors <- function(txt) {
  fcase(
    grepl("software engineer|software developer|software architect|web developer|web designer|front.?end|back.?end|full.?stack|mobile developer|android developer|ios developer|app developer|devops engineer|devops specialist|site reliability|cloud engineer|platform engineer|qa engineer|quality assurance engineer|test engineer|automation engineer|data engineer|machine learning engineer|ai engineer|nlp engineer|embedded engineer|firmware engineer|game developer|systems programmer", txt), "Software & IT Development",
    grepl("teacher|lecturer|professor|tutor|instructor|educator|trainer|teaching assistant|learning support|head teacher|principal|\\bschool\\b|university lecturer|\\bacademic\\b|curriculum|\\bfaculty\\b|\\bdean\\b|nursery teacher|primary teacher|secondary teacher|special education|eyfs|sen teacher|class teacher|form teacher|maths teacher|science teacher|pe teacher|esl teacher|tefl teacher|english language teacher", txt), "Education - Teaching",
    grepl("registered nurse|staff nurse|charge nurse|ward nurse|theatre nurse|mental health nurse|community nurse|neonatal nurse|paediatric nurse|nursing manager|nursing director|nursing assistant|healthcare assistant|\\bhca\\b|midwife|midwifery|paramedic|ambulance technician|physiotherapist|occupational therapist|speech therapist|radiographer|sonographer|dietitian|care worker|care assistant|support worker|health visitor|practice nurse|nurse practitioner|pharmacy technician|biomedical scientist", txt), "Healthcare - Nursing & Allied",
    grepl("\\bdoctor\\b|general practitioner|\\bgp\\b|physician|surgeon|specialist physician|anaesthetist|radiologist|pathologist|psychiatrist|paediatrician|cardiologist|neurologist|oncologist|dermatologist|\\bdentist\\b|dental surgeon|ophthalmologist|optometrist|\\bpharmacist\\b|medical officer|medical director|clinical specialist|clinical director|clinical lead|gp registrar|foundation doctor|junior doctor|house officer", txt), "Healthcare - Clinical",
    grepl("mechanical engineer|civil engineer|structural engineer|electrical engineer|electronic engineer|manufacturing engineer|process engineer|chemical engineer|industrial engineer|quality engineer|design engineer|aerospace engineer|automotive engineer|maintenance engineer|plant engineer|commissioning engineer|controls engineer|instrumentation engineer|production engineer|machine operator|plant operator|toolmaker|machinist|\\bfitter\\b|fabricator|assembler|welder|production supervisor|engineering manager|chief engineer|lead engineer|principal engineer", txt), "Engineering & Manufacturing",
    grepl("\\baccountant\\b|\\bauditor\\b|finance manager|financial analyst|management accountant|tax advisor|tax manager|\\bpayroll\\b|bookkeeper|credit controller|accounts payable|accounts receivable|financial controller|finance director|\\bcfo\\b|chief financial|\\btreasury\\b|budget analyst|cost accountant|chartered accountant|fp.a|financial reporting|accounts manager|finance officer|finance analyst|finance coordinator", txt), "Finance & Accounting",
    grepl("sales manager|sales director|account manager|account executive|business development|sales executive|sales representative|sales consultant|sales engineer|pre.?sales|inside sales|regional sales|national sales|area sales|territory manager|commercial manager|commercial director|revenue manager|\\bbdr\\b|\\bsdr\\b", txt), "Sales & Business Development",
    grepl("marketing manager|marketing director|brand manager|digital marketing|marketing executive|marketing analyst|\\bseo\\b|social media manager|content manager|communications manager|pr manager|public relations|communications director|campaign manager|growth manager|email marketing|performance marketing|media manager|marketing coordinator", txt), "Marketing & Communications",
    grepl("project manager|programme manager|project director|project coordinator|project lead|delivery manager|\\bpmo\\b|project management office|scrum master|agile coach|product manager|product owner|release manager|change manager|implementation manager", txt), "Project & Programme Management",
    grepl("operations manager|operations director|general manager|business manager|site manager|branch manager|centre manager|operations coordinator|operations analyst|business analyst|process improvement|\\blean\\b|six sigma|continuous improvement|facilities manager|office manager|administration manager", txt), "Operations & General Management",
    grepl("hr manager|human resources manager|hr director|recruitment consultant|\\brecruiter\\b|talent acquisition|hr business partner|hr advisor|hr officer|hr coordinator|learning and development|l.d manager|training manager|organisational development|people manager|workforce planning|compensation|benefits manager|employee relations|\\bhrbp\\b", txt), "Human Resources & Recruitment",
    grepl("it manager|it director|head of it|it support|systems administrator|network engineer|network administrator|infrastructure engineer|systems engineer|it analyst|\\bhelpdesk\\b|service desk|it technician|desktop support|it operations", txt), "IT Infrastructure & Networks",
    grepl("ict manager|ict director|technology manager|technology director|digital transformation|solutions architect|enterprise architect|it consultant|technology consultant|\\bcio\\b|\\bcto\\b|chief information|chief technology|digital director|head of technology|technical director|chief digital", txt), "ICT Management & Consulting",
    grepl("data scientist|data analyst|business intelligence|bi developer|bi analyst|data manager|analytics manager|data architect|statistician|quantitative analyst|reporting analyst|data consultant|insights analyst|research analyst|data lead", txt), "Data Science & Analytics",
    grepl("\\bcyber\\b|security analyst|security engineer|security architect|information security|penetration tester|\\bpentest\\b|soc analyst|vulnerability|threat intelligence|\\bforensic\\b|incident response|\\bgdpr\\b|data protection officer|information assurance|security manager|security consultant", txt), "Cybersecurity & Compliance",
    grepl("solicitor|barrister|\\blawyer\\b|legal advisor|legal counsel|legal manager|paralegal|legal executive|compliance manager|compliance officer|legal director|general counsel|regulatory affairs|regulatory manager|legal officer|governance manager|contract manager|legal specialist|conveyancer", txt), "Legal & Compliance",
    grepl("investment banker|investment analyst|fund manager|portfolio manager|wealth manager|financial advisor|financial planner|stockbroker|equity trader|risk analyst|risk manager|underwriter|actuary|mortgage advisor|mortgage broker|loan officer|asset manager|private equity|hedge fund|credit analyst|capital markets|banking analyst|insurance analyst", txt), "Banking, Finance & Insurance",
    grepl("social worker|community worker|probation officer|youth worker|case manager|welfare officer|housing officer|counsellor|psychotherapist|mental health support worker|disability support worker|rehabilitation specialist|child protection officer|safeguarding officer|outreach worker|family support|foster care|addiction counsellor|drug and alcohol worker|advocacy worker", txt), "Social Work & Community Services",
    grepl("construction manager|site engineer|quantity surveyor|building manager|construction director|\\barchitect\\b|urban planner|interior designer|electrician|\\bplumber\\b|carpenter|bricklayer|hvac engineer|building surveyor|building inspector|planning engineer|planning manager|estimator|scaffolder|construction worker|fit.?out manager|property manager|structural technician|architectural technician", txt), "Construction & Architecture",
    grepl("\\bceo\\b|chief executive|managing director|\\bcoo\\b|chief operating|vice president|\\bsvp\\b|\\bevp\\b|group director|global director|executive director|country manager|general director|chief officer|\\bpresident\\b|board member|chairman|non.?executive director", txt), "Senior Management & C-Suite",
    grepl("supply chain manager|supply chain director|logistics manager|procurement manager|purchasing manager|warehouse manager|inventory manager|distribution manager|transport manager|freight manager|shipping manager|materials manager|demand planner|supply planner|fulfilment manager|\\bbuyer\\b", txt), "Supply Chain & Logistics",
    grepl("retail manager|store manager|shop manager|hotel manager|hospitality manager|restaurant manager|catering manager|events manager|venue manager|tourism manager|leisure manager|food and beverage|f.b manager|front of house|bar manager|floor manager", txt), "Retail, Hospitality & Events",
    grepl("customer service manager|customer success|customer experience|call centre manager|contact centre manager|customer relations|client services manager|customer support manager|client manager|customer care|customer advisor|client advisor", txt), "Customer Service & Support",
    grepl("policy officer|civil servant|\\bgovernment\\b|\\bcouncil\\b|local authority|public sector|regulatory officer|administration officer|\\bclerk\\b|executive officer|personal assistant|executive assistant|document control|records manager|public administrator|policy analyst|parliamentary", txt), "Public Sector & Administration",
    grepl("environment|sustainability|health and safety|hse manager|ehs manager|she manager|esg manager|renewable energy|energy manager|climate change|carbon manager|waste manager|ecology|conservation|environmental manager|safety manager|environmental consultant|sustainability manager", txt), "Environment, Safety & Sustainability",
    grepl("\\bresearcher\\b|research officer|\\bscientist\\b|laboratory manager|postdoctoral|phd researcher|principal investigator|research director|research manager|research fellow|biologist|biochemist|chemist|physicist|epidemiologist|clinical researcher|r.d manager|research scientist|research associate", txt), "Research & Academia",
    grepl("graphic designer|creative director|art director|media manager|content creator|\\bjournalist\\b|\\beditor\\b|photographer|videographer|animator|illustrator|broadcast|\\bfilm\\b|copywriter|ux designer|ui designer|visual designer|interaction designer", txt), "Creative, Media & Design",
    default = "Other & Unclassified"
  )
}

broad_map <- function(txt) {
  fcase(
    grepl("teach|school|educat|tutor|lectur|instruct|learn|curriculum|classroom|pupil|student", txt), "Education - Teaching",
    grepl("nurs|care assistant|healthcare assistant|\\bhca\\b|midwif|paramedic|physiother|radiograph|dietit|pharmacist", txt), "Healthcare - Nursing & Allied",
    grepl("\\bdoctor\\b|physician|surgeon|\\bgp\\b|psychiatr|dentist|optometr|consultant physician|medical officer", txt), "Healthcare - Clinical",
    grepl("engineer|manufactur|machine|fitter|welder|assembl|production|quality inspector|commissioning", txt), "Engineering & Manufacturing",
    grepl("accountant|auditor|payroll|bookkeep|treasury|\\bcfo\\b|financial controller|fp.a|accounts payable", txt), "Finance & Accounting",
    grepl("sales|account executive|business development|\\bbdr\\b|\\bsdr\\b|territory|revenue|commercial", txt) & grepl("manager|executive|director|officer|lead|head|represent|consult", txt), "Sales & Business Development",
    grepl("market|brand|digital marketing|seo|social media|campaign|content|advertising|public relat", txt), "Marketing & Communications",
    grepl("project|programme|scrum|agile|\\bpmo\\b|product owner|delivery|sprint|waterfall|kanban", txt), "Project & Programme Management",
    grepl("operat|general manag|business manag|facilities|office manag|process|lean|six sigma", txt), "Operations & General Management",
    grepl("supply chain|logistic|procurement|purchas|warehouse|inventor|distribut|transport|freight|shipping|\\bbuyer\\b", txt), "Supply Chain & Logistics",
    grepl("retail|store|shop|hotel|hospitality|restaurant|catering|events|venue|tourism|leisure|food.beverage|bar manager", txt), "Retail, Hospitality & Events",
    grepl("customer serv|call cent|contact cent|helpdesk|support desk|client serv|customer success", txt), "Customer Service & Support",
    grepl("software|developer|programmer|coder|web dev|app dev|devops|cloud|frontend|backend|fullstack", txt), "Software & IT Development",
    grepl("hr |human resourc|recruit|talent|l.d|learning.develop|training manager|people manager|workforce", txt), "Human Resources & Recruitment",
    grepl("it support|network|infrastructure|sysadmin|server|desktop support|systems admin|helpdesk", txt), "IT Infrastructure & Networks",
    grepl("data scientist|data analyst|analytics|intelligence|insight|statistician|sql|python|tableau|reporting", txt), "Data Science & Analytics",
    grepl("construct|architect|build|civil|structural|survey|planning|estimat|site manag|scaff", txt), "Construction & Architecture",
    grepl("legal|law|solicitor|compli|regulat|gdpr|contract|barrister|paralegal|governance", txt), "Legal & Compliance",
    grepl("bank|invest|fund|wealth|insur|actuar|mortgage|underwrite|trading|broker|financial advis", txt), "Banking, Finance & Insurance",
    grepl("social work|communit|youth|welfare|outreach|counsell|psychother|safeguard|advocacy|family support", txt), "Social Work & Community Services",
    grepl("\\bceo\\b|managing director|chief executive|vice president|\\bsvp\\b|\\bcoo\\b|non.?exec|chairman|president", txt), "Senior Management & C-Suite",
    grepl("cyber|security|infosec|soc|pentest|vulnerab|threat|forensic|incident|identity access", txt), "Cybersecurity & Compliance",
    grepl("public sector|civil serv|government|council|local authority|policy|regulatory officer|clerk|parliament", txt), "Public Sector & Administration",
    grepl("environment|sustainab|health.safety|\\bhse\\b|\\behs\\b|renewable|energy manag|climate|carbon|waste|ecology", txt), "Environment, Safety & Sustainability",
    grepl("research|scientist|laborator|postdoc|phd|investigat|biolog|biochem|chemist|physicist|epidemiolog", txt), "Research & Academia",
    grepl("ict|information technolog|it manager|it director|it consultant|technology manager|digital transform|solutions architect|\\bcto\\b|\\bcio\\b", txt), "ICT Management & Consulting",
    grepl("design|graphic|creative|media|content|writer|journalist|editor|photog|video|animat|illustrat|film|broadcast|ux|ui", txt) & grepl("manager|director|specialist|lead|producer|designer", txt), "Creative, Media & Design",
    grepl("manager|officer|analyst|specialist|advisor|coordinator|director|consultant|assistant|lead|supervisor", txt), "Operations & General Management",
    default = "Other & Unclassified"
  )
}

dt <- step_time("Pass 1 - job title", {
  dt[, esco_sector := map_sectors(tolower(matched_label))]
  cat(sprintf("    'Other' after pass 1: %.1f%%\n", mean(dt$esco_sector=="Other & Unclassified")*100)); dt
})
dt <- step_time("Pass 2 - description fallback", {
  idx <- which(dt$esco_sector == "Other & Unclassified")
  cat(sprintf("    Rows to reclassify: %s\n", format(length(idx),big.mark=",")))
  if (length(idx)>0 && "matched_description" %in% names(dt)) {
    new_labels <- map_sectors(tolower(dt$matched_description[idx]))
    rescued    <- sum(new_labels != "Other & Unclassified")
    dt$esco_sector[idx] <- new_labels
    cat(sprintf("    Rescued: %s | 'Other' remaining: %.1f%%\n",
                format(rescued,big.mark=","), mean(dt$esco_sector=="Other & Unclassified")*100))
  }; dt
})
dt <- step_time("Pass 2b", {
  idx2 <- which(dt$esco_sector == "Other & Unclassified")
  cat(sprintf("    Rows to rescue: %s\n", format(length(idx2),big.mark=",")))
  if (length(idx2) > 0) {
    new_labels  <- broad_map(tolower(dt$matched_label[idx2]))
    still_other <- which(new_labels == "Other & Unclassified")
    if (length(still_other)>0 && "matched_description" %in% names(dt))
      new_labels[still_other] <- broad_map(tolower(dt$matched_description[idx2[still_other]]))
    dt$esco_sector[idx2] <- new_labels
  }
  rescued2 <- length(idx2) - sum(dt$esco_sector=="Other & Unclassified")
  cat(sprintf("    Rescued: %s | 'Other' after 2b: %.1f%%\n",
              format(rescued2,big.mark=","), mean(dt$esco_sector=="Other & Unclassified")*100)); dt
})
dt <- step_time("Pass 3 - remove Other & Unclassified", {
  before <- nrow(dt); n_other <- sum(dt$esco_sector=="Other & Unclassified")
  dt <- dt[esco_sector != "Other & Unclassified"]
  cat(sprintf("    Removed: %s (%.1f%%) | Kept: %s\n",
              format(n_other,big.mark=","), n_other/before*100, format(nrow(dt),big.mark=","))); dt
})

cat("\n  Sector distribution:\n")
sector_counts <- dt[, .N, by = esco_sector][order(-N)]
sector_counts[, pct := round(N/sum(N)*100, 1)]
print(sector_counts)
VALID_SECTORS <- unique(dt$esco_sector)

# ── Build transitions ────────────────────────────────────────────────────────
setorder(dt, person_id, year)
dt[, next_sector_raw := shift(esco_sector, type="lead"), by = person_id]
transitions <- dt[!is.na(next_sector_raw)][, next_sector := next_sector_raw]
transitions[, prev_sector := shift(esco_sector, type="lag"), by = person_id]
seq_stats <- dt[, .(n_jobs = .N), by = person_id]
cat(sprintf("\n  Users>=2 jobs: %s | Median seq: %.1f | Total transitions: %s\n",
            format(sum(seq_stats$n_jobs>=2),big.mark=","), median(seq_stats$n_jobs),
            format(nrow(transitions),big.mark=",")))

p_seq <- { sd <- as.data.frame(seq_stats); sd$nc <- pmin(sd$n_jobs,15)
  ggplot(sd, aes(x=nc)) + geom_histogram(binwidth=1,fill="steelblue",color="white",alpha=0.85) +
    scale_x_continuous(breaks=1:15,labels=c(1:14,"15+")) + scale_y_continuous(labels=comma) +
    labs(title="Career Sequence Length Distribution",x="Number of classified jobs",y="Number of users") +
    theme_minimal(base_size=12) + theme(plot.title=element_text(face="bold")) }
ggsave("output/sequence_length_distribution.png", p_seq, width=10, height=5, dpi=150)
cat("  Saved: output/sequence_length_distribution.png\n")
cat(sprintf("  Sectors: %s | Unique pairs: %s | Sparsity: %.2f%%\n",
            uniqueN(transitions$esco_sector),
            uniqueN(paste(transitions$esco_sector,transitions$next_sector)),
            (1-uniqueN(paste(transitions$esco_sector,transitions$next_sector))/uniqueN(transitions$esco_sector)^2)*100))

# ── University flag ──────────────────────────────────────────────────────────
cat("\n>>> University flag (majority vote)...\n")
if ("university_studies" %in% names(dt)) {
  pu <- dt[, .(univ_flag=(sum(university_studies==TRUE,na.rm=TRUE)/pmax(sum(!is.na(university_studies)),1))>=0.5), by=person_id]
  person_univ <- setNames(pu$univ_flag, pu$person_id)
  transitions[, univ_flag := person_univ[as.character(person_id)]]
  transitions[is.na(univ_flag), univ_flag := FALSE]
  cat(sprintf("    Uni: %.1f%% | No uni: %.1f%%\n", mean(person_univ)*100, mean(!person_univ)*100))
} else {
  transitions[, univ_flag := FALSE]
  person_univ <- setNames(rep(FALSE, uniqueN(dt$person_id)), as.character(unique(dt$person_id)))
}

# =============================================================================
# 4. PCA
# =============================================================================
cat("\n== 4. PCA ==\n\n")
asf <- sort(unique(c(transitions$esco_sector, transitions$next_sector)))
nsf <- length(asf); si2 <- setNames(seq_len(nsf), asf)
cmf <- step_time("Building transition count matrix for PCA", {
  ct  <- transitions[,.N,by=.(esco_sector,next_sector)]
  mat <- matrix(0,nsf,nsf,dimnames=list(asf,asf))
  mat[cbind(si2[ct$esco_sector],si2[ct$next_sector])] <- ct$N
  mat <- mat+0.01; mat/rowSums(mat)
})
pca_res <- step_time("Running PCA", { prcomp(cmf,center=TRUE,scale.=TRUE) })
pca_var <- round(summary(pca_res)$importance[2,1:5]*100,1)
cat(sprintf("  Variance: PC1:%.1f%% PC2:%.1f%% PC3:%.1f%% PC4:%.1f%% PC5:%.1f%% | Cum(1+2):%.1f%%\n",
            pca_var[1],pca_var[2],pca_var[3],pca_var[4],pca_var[5],pca_var[1]+pca_var[2]))
pdf <- data.frame(sector=rownames(pca_res$x),PC1=pca_res$x[,1],PC2=pca_res$x[,2])
pdf$group <- fcase(
  pdf$sector %in% c("Software & IT Development","Data Science & Analytics","Cybersecurity & Compliance","IT Infrastructure & Networks","ICT Management & Consulting"),"Technology",
  pdf$sector %in% c("Healthcare - Clinical","Healthcare - Nursing & Allied","Social Work & Community Services","Research & Academia"),"Health & Science",
  pdf$sector %in% c("Finance & Accounting","Banking, Finance & Insurance","Legal & Compliance"),"Finance & Legal",
  pdf$sector %in% c("Education - Teaching","Public Sector & Administration","Environment, Safety & Sustainability"),"Public & Education",
  pdf$sector %in% c("Sales & Business Development","Marketing & Communications","Customer Service & Support","Senior Management & C-Suite","Operations & General Management","Human Resources & Recruitment","Project & Programme Management"),"Business & Management",
  default="Industry & Trades"
)
p_pca <- ggplot(pdf,aes(x=PC1,y=PC2,color=group,label=sector)) + geom_point(size=3.5,alpha=0.85) +
  scale_color_manual(values=c("Technology"="steelblue","Health & Science"="#1D9E75","Finance & Legal"="#BA7517","Public & Education"="#993556","Business & Management"="#534AB7","Industry & Trades"="#A32D2D")) +
  labs(title="PCA of Sector Career-Transition Profiles",subtitle=sprintf("PC1:%.1f%% | PC2:%.1f%% variance",pca_var[1],pca_var[2]),x="PC1",y="PC2",color="Sector group") +
  theme_minimal(base_size=11) + theme(plot.title=element_text(face="bold",size=13),legend.position="right")
tryCatch({
  ggsave("output/pca_sector_profiles.png",
    p_pca+ggrepel::geom_text_repel(size=2.8,max.overlaps=20,segment.color="gray70",segment.size=0.3),
    width=13,height=9,dpi=150)
}, error=function(e) ggsave("output/pca_sector_profiles.png",p_pca,width=13,height=9,dpi=150))
cat("  Saved: output/pca_sector_profiles.png\n")

# =============================================================================
# 5. PREDICTION
# =============================================================================
cat("\n5. PREDICTION\n\n")

# Stratified split
cat(">>> Stratified train/val/test split by dominant sector...\n")
user_dom <- dt[, .(dom=esco_sector[which.max(tabulate(match(esco_sector,VALID_SECTORS)))]), by=person_id]
set.seed(42)
user_dom[, split := { n<-.N; ord<-sample(n)
  ifelse(ord<=floor(n*0.64),"train",ifelse(ord<=floor(n*0.80),"val","test")) }, by=dom]
train_users <- user_dom[split=="train", person_id]
val_users   <- user_dom[split=="val",   person_id]
test_users  <- user_dom[split=="test",  person_id]
train_data  <- transitions[person_id %in% train_users]
val_data    <- transitions[person_id %in% val_users]
test_data   <- transitions[person_id %in% test_users]
cat(sprintf("  Train: %s | Val: %s | Test: %s\n",
            format(nrow(train_data),big.mark=","), format(nrow(val_data),big.mark=","),
            format(nrow(test_data),big.mark=",")))

SMOOTHING   <- 0.01
all_sectors <- sort(unique(c(train_data$esco_sector, train_data$next_sector)))
n_sec       <- length(all_sectors)
sector_map  <- setNames(seq_len(n_sec), all_sectors)
all_jobs    <- all_sectors; n_jobs <- n_sec; job_index <- sector_map

# Sector stickiness
cat("\n>>> Sector stickiness (empirical self-loop rates)...\n")
sr_dt        <- train_data[, .(self_rate=mean(esco_sector==next_sector)), by=esco_sector]
self_rates   <- setNames(sr_dt$self_rate, sr_dt$esco_sector)
for (s in all_sectors) if (is.na(self_rates[s])) self_rates[s] <- 0.2
self_loop_mult <- 1.0 + 2.0 * self_rates
cat("    Top 5 stickiest:\n")
for (nm in names(head(sort(self_rates,decreasing=TRUE),5)))
  cat(sprintf("      %-42s : %.3f\n", nm, self_rates[nm]))

# Build transition matrix with adaptive smoothing
build_trans_mat <- function(data, weights=NULL) {
  mat <- matrix(0, n_jobs, n_jobs, dimnames=list(all_jobs,all_jobs))
  if (is.null(weights)) {
    ct <- data[,.N,by=.(esco_sector,next_sector)]
    mat[cbind(job_index[ct$esco_sector],job_index[ct$next_sector])] <- ct$N
  } else {
    ct <- data[,.(w=sum(get(weights))),by=.(esco_sector,next_sector)]
    mat[cbind(job_index[ct$esco_sector],job_index[ct$next_sector])] <- ct$w
  }
  for (s in all_sectors) { si<-job_index[s]; if(!is.na(si)) diag(mat)[si]<-diag(mat)[si]*self_loop_mult[s] }
  rt <- rowSums(mat)
  for (i in seq_len(n_jobs)) { ps<-if(rt[i]<10)0.50 else if(rt[i]<100)0.10 else SMOOTHING; mat[i,]<-mat[i,]+ps }
  mat/rowSums(mat)
}

global_matrix <- step_time("Global transition matrix", { build_trans_mat(train_data) })

DECAY_RATE <- 0.12; max_yr <- max(train_data$year,na.rm=TRUE)
recency_matrix <- step_time("Recency-weighted matrix (decay=0.12)", {
  td <- copy(train_data); td[,recency_weight:=exp(DECAY_RATE*(year-max_yr))]
  build_trans_mat(td,weights="recency_weight")
})

# Per-decade matrices
years <- sort(unique(train_data$year)); WINDOW_SIZE <- 10
transition_matrices <- step_time("Per-decade transition matrices", {
  tw<-list(); i<-1
  while(i<=length(years)){tw<-c(tw,list(c(years[i],years[min(i+WINDOW_SIZE-1,length(years))]))); i<-i+WINDOW_SIZE}
  cat("\n"); cat(paste(rep("-",42),collapse=""),"\n")
  cat(sprintf("%-15s | %-10s | %s\n","Years","Sparsity","Status"))
  cat(paste(rep("-",42),collapse=""),"\n")
  mats<-list()
  for(win in tw){
    start<-win[1]; end<-win[2]; label<-sprintf("%s-%s",start,end)
    wd<-train_data[year>=start & year<=end]
    if(nrow(wd)==0){cat(sprintf("%-15s | %10s | Empty\n",label,"N/A"));next}
    raw<-matrix(0,n_jobs,n_jobs,dimnames=list(all_jobs,all_jobs))
    ct<-wd[,.N,by=.(esco_sector,next_sector)]
    raw[cbind(job_index[ct$esco_sector],job_index[ct$next_sector])]<-ct$N
    for(s in all_sectors){si<-job_index[s];if(!is.na(si))diag(raw)[si]<-diag(raw)[si]*self_loop_mult[s]}
    wsp<-sum(raw==0)/n_jobs^2*100; sv<-if(wsp>70)0.5 else if(wsp>30)0.1 else SMOOTHING
    raw<-raw+sv; mats[[label]]<-list(mat=raw/rowSums(raw),start=start,end=end)
    cat(sprintf("%-15s | %9.2f%% | Built\n",label,wsp))
  }
  mats
})

# Market drift
drift_scores<-list(); ks<-names(transition_matrices)
for(i in seq(2,length(ks))){ df<-transition_matrices[[ks[i]]]$mat-transition_matrices[[ks[i-1]]]$mat; drift_scores[[ks[i]]]<-sqrt(sum(df^2)) }
cat("\n  Market drift:\n"); for(nm in names(drift_scores)) cat(sprintf("    %-15s : %.4f\n",nm,drift_scores[[nm]]))

# 2nd order with back-off
MIN_BIGRAM <- 5L
second_order_probs <- step_time("2nd-order Markov with back-off", {
  so  <- train_data[!is.na(prev_sector) & prev_sector %in% all_sectors]
  cat(sprintf("    Bigram pairs: %s\n",format(nrow(so),big.mark=",")))
  soc <- so[,.N,by=.(prev_sector,esco_sector,next_sector)]
  pt  <- so[,.N,by=.(prev_sector,esco_sector)]
  ptv <- setNames(pt$N,paste(pt$prev_sector,pt$esco_sector,sep="|"))
  sk  <- names(ptv); sl <- vector("list",length(sk)); names(sl)<-sk; nb<-0L
  for(key in sk){
    if(ptv[key]<MIN_BIGRAM){nb<-nb+1L;next}
    pp<-strsplit(key,"\\|")[[1]]; ps<-pp[1]; cs<-pp[2]
    sub<-soc[prev_sector==ps & esco_sector==cs]
    vec<-rep(SMOOTHING,n_sec); names(vec)<-all_sectors
    vec[sector_map[sub$next_sector]]<-sub$N+SMOOTHING
    if(cs %in% all_sectors){si<-sector_map[cs];vec[si]<-vec[si]*self_loop_mult[cs]}
    sl[[key]]<-vec/sum(vec)
  }
  cat(sprintf("    Back-off (<%d obs): %d\n",MIN_BIGRAM,nb)); sl
})


cat("\nBuilding vectorised user history priors \n")

HIST_DECAY <- 0.15   # recent jobs weighted more
# Compute per-user, per-sector weighted count from ALL train transitions
user_hist_raw <- train_data[, .(
  person_id  = person_id,
  sector     = esco_sector,
  yr         = year
)]
user_hist_raw[, rw := exp(HIST_DECAY * (yr - max_yr))]   # recency weight

# Sum recency-weighted counts per (person_id, sector)
user_sect_wt <- user_hist_raw[, .(wt_count = sum(rw)), by = .(person_id, sector)]

# Build lookup: person_id (integer) -> named probability vector over all_sectors
cat("    Computing history vectors per user...\n")
user_hist_lookup <- list()   # key = as.character(person_id)
user_tot <- user_sect_wt[, .(total = sum(wt_count)), by = person_id]
user_sect_wt <- merge(user_sect_wt, user_tot, by = "person_id")
# Keep only users with at least 2 different sector observations
multi_users <- user_tot[total >= 2, person_id]
user_sect_sub <- user_sect_wt[person_id %in% multi_users]

# Vectorised build: create a sparse matrix then normalise
for (uid in multi_users) {
  rows  <- user_sect_sub[person_id == uid]
  vec   <- rep(0.01, n_jobs); names(vec) <- all_jobs
  si    <- job_index[rows$sector]
  valid <- !is.na(si)
  if (any(valid)) vec[si[valid]] <- vec[si[valid]] + rows$wt_count[valid]
  user_hist_lookup[[as.character(uid)]] <- vec / sum(vec)
}
cat(sprintf("    History vectors built for %s users\n", format(length(user_hist_lookup),big.mark=",")))

# University boost vectors
cat("\n>>> University boost vectors...\n")
univ_boost_TRUE  <- rep(1.0,n_jobs); names(univ_boost_TRUE)  <- all_jobs
univ_boost_FALSE <- rep(1.0,n_jobs); names(univ_boost_FALSE) <- all_jobs
if (any(transitions$univ_flag,na.rm=TRUE)) {
  tru <- train_data[univ_flag==TRUE]; tnu <- train_data[univ_flag==FALSE]
  du  <- tru[,.N,by=next_sector]; dn <- tnu[,.N,by=next_sector]; dall <- train_data[,.N,by=next_sector]
  tu  <- nrow(tru); tn <- nrow(tnu); ta <- nrow(train_data)
  for(s in all_sectors){
    si <- job_index[s]
    pa  <- (dall[next_sector==s,N][[1]]+SMOOTHING)/(ta+n_sec*SMOOTHING)
    pu2 <- (du[next_sector==s,  N][[1]]+SMOOTHING)/(tu+n_sec*SMOOTHING)
    pn2 <- (dn[next_sector==s,  N][[1]]+SMOOTHING)/(tn+n_sec*SMOOTHING)
    if(length(pa)==0)  pa  <- SMOOTHING/ta
    if(length(pu2)==0) pu2 <- SMOOTHING/tu
    if(length(pn2)==0) pn2 <- SMOOTHING/tn
    univ_boost_TRUE[si]  <- pmin(pmax(pu2/pa,0.5),2.0)
    univ_boost_FALSE[si] <- pmin(pmax(pn2/pa,0.5),2.0)
  }
  cat("    Top 5 boosted for uni users:\n")
  for(nm in names(head(sort(univ_boost_TRUE,decreasing=TRUE),5)))
    cat(sprintf("      %-42s : %.3f\n",nm,univ_boost_TRUE[nm]))
}

# Archetype clustering
cat("\n>>> User archetypes...\n")
risk_tr <- {
  civ<-job_index[train_data$esco_sector]; niv<-job_index[train_data$next_sector]
  vld<-!is.na(civ)&!is.na(niv); lp<-rep(log(1e-9),nrow(train_data))
  if(any(vld)) lp[vld]<-log(pmax(global_matrix[cbind(civ[vld],niv[vld])],1e-9))
  train_data[,log_prob:=lp]
  train_data[,.(drift_score=-mean(log_prob),seq_length=.N,
                self_loop_rate=mean(esco_sector==next_sector)),by=person_id][seq_length>=2]
}
set.seed(42)
kmf <- risk_tr[,.(d=scale(drift_score)[,1],l=scale(seq_length)[,1],s=scale(self_loop_rate)[,1])]
kmf <- kmf[complete.cases(kmf)]
km5 <- kmeans(kmf,centers=5,nstart=25,iter.max=100)
risk_tr$cluster<-NA_integer_; risk_tr$cluster[seq_len(nrow(kmf))]<-km5$cluster
cs <- risk_tr[!is.na(cluster),.(mean_drift=mean(drift_score),mean_len=mean(seq_length),mean_loop=mean(self_loop_rate)),by=cluster]
cs[,archetype:=fcase(
  mean_drift>quantile(mean_drift,0.8)&mean_loop<0.3,"Career Switcher",
  mean_drift<quantile(mean_drift,0.2)&mean_loop>0.5,"Sector Loyalist",
  mean_len>quantile(mean_len,0.75),"Career Veteran",
  mean_drift>quantile(mean_drift,0.5),"Gradual Mover",
  default="Stable Specialist"
)]
cat("    Archetype labels:\n"); print(cs[,.(cluster,archetype,mean_drift,mean_loop)])
user_cluster <- merge(risk_tr[,.(person_id,cluster)],cs[,.(cluster,archetype)],by="cluster")
train_data   <- merge(train_data,user_cluster[,.(person_id,archetype)],by="person_id",all.x=TRUE)
train_data[is.na(archetype),archetype:="Stable Specialist"]
train_data[,univ_flag:=person_univ[as.character(person_id)]]
train_data[is.na(univ_flag),univ_flag:=FALSE]
archetype_matrices <- step_time("Per-archetype matrices", {
  am<-list()
  for(arch in unique(cs$archetype)){
    sub<-train_data[archetype==arch]
    am[[arch]]<-if(nrow(sub)>=50){cat(sprintf("    %-25s: %d\n",arch,nrow(sub)));build_trans_mat(sub)} else global_matrix
  }; am
})

get_win_key <- function(yr){
  for(key in names(transition_matrices)){w<-transition_matrices[[key]];if(yr>=w$start&yr<=w$end)return(key)}
  "global"
}

cat("\n Tuning per-sector ensemble weights on validation set...\n")

val_data[, win_key   := vapply(year, get_win_key, character(1))]
val_data[, univ_flag := person_univ[as.character(person_id)]]
val_data[is.na(univ_flag), univ_flag := FALSE]
val_data <- merge(val_data, user_cluster[,.(person_id,archetype)], by="person_id", all.x=TRUE)
val_data[is.na(archetype), archetype := "Stable Specialist"]

w_hist_g <- c(0.10, 0.20, 0.30, 0.40, 0.50)
w_so_g   <- c(0.00, 0.10, 0.20, 0.30)
w_arch_g <- c(0.05, 0.10, 0.20)

# Sector-conditional defaults based on stickiness
sector_best_w <- lapply(all_sectors, function(s) {
  sr <- self_rates[s]
  if (!is.na(sr) && sr > 0.25) list(w_hist=0.30, w_so=0.10, w_arch=0.10)  # sticky: rely more on history
  else if (!is.na(sr) && sr < 0.10) list(w_hist=0.20, w_so=0.30, w_arch=0.10)  # mobile: rely on 2nd order
  else list(w_hist=0.20, w_so=0.20, w_arch=0.10)
})
names(sector_best_w) <- all_sectors

for (curr_s in all_sectors) {
  sub_val <- val_data[esco_sector == curr_s]
  if (nrow(sub_val) < 15) next
  if (nrow(sub_val) > 2000) sub_val <- sub_val[sample(.N, 2000)]
  ci <- job_index[curr_s]; if (is.na(ci)) next

  best_acc <- -1.0
  best_w   <- sector_best_w[[curr_s]]

  for (wh in w_hist_g) for (ws in w_so_g) for (wa in w_arch_g) {
    w_rest <- 1.0 - wh - ws - wa; if (w_rest < 0.05) next
    hits <- vapply(seq_len(nrow(sub_val)), function(i) {
      row  <- sub_val[i]; wk<-row$win_key; prev<-row$prev_sector
      arch <- row$archetype; univ<-row$univ_flag; uid<-as.character(row$person_id)
      dyn_p  <- if(wk=="global"||!(wk%in%names(transition_matrices))) global_matrix[ci,] else transition_matrices[[wk]]$mat[ci,]
      rec_p  <- recency_matrix[ci,]
      arch_p <- if(!is.null(archetype_matrices[[arch]])) archetype_matrices[[arch]][ci,] else global_matrix[ci,]
      so_key <- if(!is.na(prev)&&prev%in%all_sectors) paste(prev,curr_s,sep="|") else NA_character_
      so_p   <- if(!is.na(so_key)&&!is.null(second_order_probs[[so_key]])) second_order_probs[[so_key]] else global_matrix[ci,]
      # [FIX 1] lookup by person_id string
      hist_p <- if(!is.null(user_hist_lookup[[uid]])) user_hist_lookup[[uid]] else global_matrix[ci,]
      base   <- 0.60*rec_p + 0.40*dyn_p
      ens_p  <- w_rest*base + wa*arch_p + ws*so_p + wh*hist_p
      boost  <- if(isTRUE(univ)) univ_boost_TRUE else univ_boost_FALSE
      ens_p  <- ens_p*boost; ens_p<-ens_p/sum(ens_p)
      row$next_sector %in% all_jobs[order(ens_p,decreasing=TRUE)[1L]]
    }, logical(1))
    acc <- mean(hits)
    if (acc > best_acc) { best_acc <- acc; best_w <- list(w_hist=wh,w_so=ws,w_arch=wa) }
  }
  sector_best_w[[curr_s]] <- best_w
}
wdf <- do.call(rbind,lapply(names(sector_best_w),function(s){w<-sector_best_w[[s]];data.frame(sector=s,w_hist=w$w_hist,w_so=w$w_so,w_arch=w$w_arch)}))
cat(sprintf("  w_hist dist: %s\n  w_so dist  : %s\n  w_arch dist: %s\n",
            paste(table(wdf$w_hist),collapse=" | "), paste(table(wdf$w_so),collapse=" | "),
            paste(table(wdf$w_arch),collapse=" | ")))

# =============================================================================
# EVALUATION
# =============================================================================
TOP_K_LIST <- c(1,3,5); max_k <- max(TOP_K_LIST)
test_data[, win_key   := vapply(year, get_win_key, character(1))]
test_data[, univ_flag := person_univ[as.character(person_id)]]
test_data[is.na(univ_flag), univ_flag := FALSE]
test_data <- merge(test_data,user_cluster[,.(person_id,archetype)],by="person_id",all.x=TRUE)
test_data[is.na(archetype), archetype := "Stable Specialist"]

persistence_acc <- mean(test_data$esco_sector == test_data$next_sector)
cat(sprintf("\n  Persistence Baseline Top-1: %.2f%%\n", persistence_acc*100))

ensemble_topk <- function(ds) {
  lapply(seq_len(nrow(ds)), function(i) {
    row  <- ds[i]; curr<-row$esco_sector; prev<-row$prev_sector; wk<-row$win_key
    arch <- row$archetype; univ<-row$univ_flag; uid<-as.character(row$person_id)
    ci   <- job_index[curr]; if(is.na(ci)) return(all_jobs[seq_len(max_k)])
    w    <- sector_best_w[[curr]]; if(is.null(w)) w<-list(w_hist=0.20,w_so=0.20,w_arch=0.10)
    wh<-w$w_hist; ws<-w$w_so; wa<-w$w_arch; w_rest<-max(0.05,1.0-wh-ws-wa)
    dyn_p  <- if(wk=="global"||!(wk%in%names(transition_matrices))) global_matrix[ci,] else transition_matrices[[wk]]$mat[ci,]
    rec_p  <- recency_matrix[ci,]
    arch_p <- if(!is.null(archetype_matrices[[arch]])) archetype_matrices[[arch]][ci,] else global_matrix[ci,]
    so_key <- if(!is.na(prev)&&prev%in%all_sectors) paste(prev,curr,sep="|") else NA_character_
    so_p   <- if(!is.na(so_key)&&!is.null(second_order_probs[[so_key]])) second_order_probs[[so_key]] else global_matrix[ci,]
    hist_p <- if(!is.null(user_hist_lookup[[uid]])) user_hist_lookup[[uid]] else global_matrix[ci,]
    base   <- 0.60*rec_p + 0.40*dyn_p
    ens_p  <- w_rest*base + wa*arch_p + ws*so_p + wh*hist_p
    boost  <- if(isTRUE(univ)) univ_boost_TRUE else univ_boost_FALSE
    ens_p  <- ens_p*boost; ens_p<-ens_p/sum(ens_p)
    all_jobs[order(ens_p,decreasing=TRUE)[seq_len(max_k)]]
  })
}

valid_test       <- test_data[esco_sector %in% all_jobs]
ensemble_results <- step_time("Full ensemble model accuracy", {
  preds <- ensemble_topk(valid_test)
  setNames(vapply(TOP_K_LIST,function(k)
    mean(mapply(function(a,p) a%in%p[seq_len(k)],valid_test$next_sector,preds)),
    numeric(1)),paste0("top_",TOP_K_LIST))
})

cat("\n"); cat(paste(rep("=",60),collapse=""),"\n")
cat(sprintf("  %-22s | %-8s | %-8s | %-8s\n","Model","Top-1","Top-3","Top-5"))
cat(paste(rep("-",60),collapse=""),"\n")
cat(sprintf("  %-22s | %7.2f%% | %7s  | %7s\n","Persistence Baseline",persistence_acc*100,"NA","NA"))
cat(sprintf("  %-22s | %7.2f%% | %7.2f%% | %7.2f%%\n","Ensemble (pred11)",
            ensemble_results["top_1"]*100,ensemble_results["top_3"]*100,ensemble_results["top_5"]*100))
cat(paste(rep("-",60),collapse=""),"\n")
cat(sprintf("  Ensemble vs Baseline : %+.2f%%\n",(ensemble_results["top_1"]-persistence_acc)*100))
cat(paste(rep("=",60),collapse=""),"\n")

# =============================================================================
# 6. CLUSTERING
# =============================================================================
cat("\n6. CLUSTERING \n\n")
risk_df <- step_time("Career drift scores", {
  civ<-job_index[transitions$esco_sector]; niv<-job_index[transitions$next_sector]
  vld<-!is.na(civ)&!is.na(niv); lp<-rep(log(1e-9),nrow(transitions))
  if(any(vld)) lp[vld]<-log(pmax(global_matrix[cbind(civ[vld],niv[vld])],1e-9))
  transitions[,log_prob:=lp]
  risk<-transitions[,.(drift_score=-mean(log_prob),total_transitions=.N,
                       self_loop_rate=mean(esco_sector==next_sector)),by=person_id][total_transitions>=2]
  setorder(risk,-drift_score); risk
})
dom_sec  <- dt[,.(dominant_sector=esco_sector[which.max(tabulate(match(esco_sector,VALID_SECTORS)))]),by=person_id]
risk_df  <- merge(merge(risk_df,dom_sec,by="person_id",all.x=TRUE),dt[,.(seq_length=.N),by=person_id],by="person_id",all.x=TRUE)
cat(sprintf("  Users: %s | Median drift: %.4f\n",format(nrow(risk_df),big.mark=","),median(risk_df$drift_score)))
kmf2 <- risk_df[,.(d=scale(drift_score)[,1],l=scale(seq_length)[,1],s=scale(self_loop_rate)[,1])]
kmf2 <- kmf2[complete.cases(kmf2)]; set.seed(42)
km2  <- step_time("k-means (k=5)",{kmeans(kmf2,centers=5,nstart=25,iter.max=100)})
risk_df$cluster<-NA_integer_; risk_df$cluster[seq_len(nrow(kmf2))]<-km2$cluster
cat(sprintf("  WCSS:%.2f | Between-SS:%.2f | Quality:%.3f\n",km2$tot.withinss,km2$betweenss,km2$betweenss/km2$totss))
cs2 <- risk_df[!is.na(cluster),.(n_users=.N,mean_drift=round(mean(drift_score),3),mean_len=round(mean(seq_length),1),mean_loop=round(mean(self_loop_rate),3)),by=cluster][order(cluster)]
print(cs2)
cs2[,archetype:=fcase(
  mean_drift>quantile(mean_drift,0.8)&mean_loop<0.3,"Career Switcher",
  mean_drift<quantile(mean_drift,0.2)&mean_loop>0.5,"Sector Loyalist",
  mean_len>quantile(mean_len,0.75),"Career Veteran",
  mean_drift>quantile(mean_drift,0.5),"Gradual Mover",
  default="Stable Specialist"
)]
cat("\n  Archetype labels:\n"); print(cs2[,.(cluster,archetype,n_users,mean_drift,mean_loop)])
pdf2 <- merge(risk_df[!is.na(cluster)],cs2[,.(cluster,archetype)],by="cluster")
p_cl <- ggplot(as.data.frame(pdf2),aes(x=drift_score,y=seq_length,color=archetype)) +
  geom_point(alpha=0.35,size=0.8) +
  geom_point(data=as.data.frame(cs2),aes(x=mean_drift,y=mean_len,color=archetype),size=5,shape=18,inherit.aes=FALSE) +
  scale_color_brewer(palette="Set1") +
  labs(title="User Career Clusters (k-means, k=5)",x="Drift Score",y="Sequence Length",color="Archetype") +
  theme_minimal(base_size=12)+theme(plot.title=element_text(face="bold",size=13),legend.position="right")
ggsave("output/user_clusters.png",p_cl,width=12,height=7,dpi=150); cat("  Saved: output/user_clusters.png\n")
tsp <- merge(risk_df[!is.na(cluster),.(n=.N),by=.(cluster,dominant_sector)][order(cluster,-n)][,head(.SD,3),by=cluster],cs2[,.(cluster,archetype)],by="cluster")
p_cl2 <- ggplot(as.data.frame(tsp),aes(x=reorder(dominant_sector,n),y=n,fill=archetype)) + geom_col(alpha=0.85) +
  facet_wrap(~paste0("Cluster ",cluster,": ",archetype),scales="free_y") + coord_flip() + scale_fill_brewer(palette="Set1") +
  labs(title="Top Sectors per Career Archetype Cluster",x=NULL,y="Users") +
  theme_minimal(base_size=10)+theme(plot.title=element_text(face="bold",size=12),strip.text=element_text(face="bold"))
ggsave("output/cluster_sector_breakdown.png",p_cl2,width=14,height=8,dpi=150); cat("  Saved: output/cluster_sector_breakdown.png\n")
cat("\n  TOP 10 HIGHEST DRIFT RISK:\n"); print(head(risk_df[,.(person_id,drift_score,total_transitions)],10))
fwrite(risk_df,"output/drift_risk_scores.csv"); cat("  Saved: output/drift_risk_scores.csv\n")

# =============================================================================
# 7. ASSOCIATION RULES
# =============================================================================
cat("\n7. ASSOCIATION RULES \n\n")
assoc_rules <- step_time("Computing support, confidence, lift", {
  tot<-nrow(transitions)
  pc<-transitions[,.N,by=.(esco_sector,next_sector)]; setnames(pc,"N","pair_count")
  ac<-transitions[,.N,by=esco_sector]; setnames(ac,"N","ant_count")
  cc<-transitions[,.N,by=next_sector]; setnames(cc,"N","con_count")
  r<-merge(merge(pc,ac,by="esco_sector"),cc,by="next_sector")
  r[,support:=pair_count/tot][,confidence:=pair_count/ant_count][,lift:=confidence/(con_count/tot)]
  r[,.(from=esco_sector,to=next_sector,support=round(support,4),confidence=round(confidence,4),lift=round(lift,4),count=pair_count)][order(-lift)]
})
cat(sprintf("  Total rules: %s\n",format(nrow(assoc_rules),big.mark=",")))
cat("\n  TOP 15 RULES BY LIFT:\n");       print(assoc_rules[1:15])
cat("\n  TOP 15 RULES BY CONFIDENCE:\n"); print(assoc_rules[order(-confidence)][1:15])
cat("\n  TOP 15 RULES BY SUPPORT:\n");   print(assoc_rules[order(-support)][1:15])
fwrite(assoc_rules,"output/association_rules.csv"); cat("  Saved: output/association_rules.csv\n")
p_lift <- { top20<-assoc_rules[from!=to][order(-lift)][1:min(20,.N)]
  top20[,rule:=factor(paste0(str_trunc(from,22)," ->\n",str_trunc(to,22)),
    levels=rev(paste0(str_trunc(from,22)," ->\n",str_trunc(to,22))))]
  ggplot(as.data.frame(top20),aes(x=lift,y=rule,fill=confidence)) + geom_col(alpha=0.9) +
    scale_fill_gradient(low="#B5D4F4",high="#042C53",name="Confidence") +
    geom_text(aes(label=sprintf("lift=%.2f",lift)),hjust=-0.1,size=3) +
    scale_x_continuous(expand=expansion(mult=c(0,0.15))) +
    labs(title="Top 20 Cross-Sector Transitions by Lift",subtitle="Lift > 1 = more likely than chance",x="Lift",y=NULL) +
    theme_minimal(base_size=10)+theme(plot.title=element_text(face="bold",size=12)) }
ggsave("output/association_rules_lift.png",p_lift,width=13,height=9,dpi=150); cat("  Saved: output/association_rules_lift.png\n")

# =============================================================================
# 8. VISUALIZATION
# =============================================================================
cat("\n8. VISUALIZATION\n\n")

# Sector distribution
p_sec <- { sd<-copy(sector_counts); sd[,esco_sector:=factor(esco_sector,levels=esco_sector[order(N)])]
  ggplot(as.data.frame(sd),aes(x=N,y=esco_sector)) + geom_col(fill="steelblue",alpha=0.85) +
    geom_text(aes(label=paste0(pct,"%")),hjust=-0.1,size=3) +
    labs(title="Job Distribution Across 27 ESCO Sectors (Other Removed)",x="Records",y=NULL) +
    scale_x_continuous(expand=expansion(mult=c(0,0.12)),labels=comma) +
    theme_minimal(base_size=10)+theme(plot.title=element_text(face="bold",size=12)) }
ggsave("output/sector_distribution.png",p_sec,width=13,height=9,dpi=150); cat("  Saved: output/sector_distribution.png\n")

# Career drift distribution
p_driftp <- { mv<-median(risk_df$drift_score)
  ggplot(as.data.frame(risk_df),aes(x=drift_score)) +
    geom_histogram(bins=50,fill="gray55",color="white",alpha=0.85) +
    geom_vline(xintercept=mv,color="red",linetype="dashed",linewidth=1.1) +
    annotate("text",x=mv+0.05,y=Inf,vjust=2,label=sprintf("Median: %.2f",mv),color="red",hjust=0,size=4) +
    labs(title="Career Drift Distribution (27 Sectors)",x="Drift Score (higher = more volatile)",y="Number of Users") +
    theme_minimal(base_size=12)+theme(plot.title=element_text(face="bold",size=14)) }
ggsave("output/career_drift_distribution.png",p_driftp,width=10,height=6,dpi=150); cat("  Saved: output/career_drift_distribution.png\n")

# Top transitions
p_top <- { tt<-transitions[,.N,by=.(esco_sector,next_sector)][order(-N)][1:20]
  tt[,pair:=factor(paste0(str_trunc(esco_sector,25)," ->\n",str_trunc(next_sector,25)),
    levels=rev(paste0(str_trunc(esco_sector,25)," ->\n",str_trunc(next_sector,25))))]
  ggplot(as.data.frame(tt),aes(x=N,y=pair)) + geom_col(fill="steelblue",alpha=0.85) +
    scale_x_continuous(labels=comma) + labs(title="Top 20 Most Common Career Transitions",x="Count",y=NULL) +
    theme_minimal(base_size=10)+theme(plot.title=element_text(face="bold",size=12)) }
ggsave("output/top_transitions.png",p_top,width=13,height=9,dpi=150); cat("  Saved: output/top_transitions.png\n")

p_acc <- {
  df_acc <- data.frame(
    Metric = factor(c("Top-1","Top-3","Top-5"), levels=c("Top-1","Top-3","Top-5")),
    Ensemble = c(ensemble_results["top_1"], ensemble_results["top_3"], ensemble_results["top_5"])*100,
    Baseline = c(persistence_acc, NA, NA)*100
  )
  ggplot(df_acc, aes(x=Metric)) +
    geom_col(aes(y=Ensemble), fill="#1f77b4", alpha=0.9, width=0.5) +
    geom_text(aes(y=Ensemble, label=sprintf("%.1f%%",Ensemble)), vjust=-0.4, size=5, fontface="bold", color="#1f77b4") +
    geom_hline(yintercept=persistence_acc*100, color="red", linetype="dashed", linewidth=1.2) +
    annotate("text", x=0.6, y=persistence_acc*100+1.5,
             label=sprintf("Persistence Baseline: %.2f%%", persistence_acc*100),
             color="red", hjust=0, size=4) +
    annotate("text", x=1, y=ensemble_results["top_1"]*100/2,
             label=sprintf("+%.2f%%\nabove\nbaseline",
                           (ensemble_results["top_1"]-persistence_acc)*100),
             color="white", size=4, fontface="bold") +
    scale_y_continuous(limits=c(0,100), labels=function(x) paste0(x,"%")) +
    labs(title="Career Prediction Accuracy — Ensemble Model (pred11)",
         subtitle=sprintf("Baseline: %.2f%% | Top-1: %.2f%% | Top-3: %.2f%% | Top-5: %.2f%%",
                          persistence_acc*100,
                          ensemble_results["top_1"]*100,
                          ensemble_results["top_3"]*100,
                          ensemble_results["top_5"]*100),
         x="Prediction Task", y="Accuracy (%)") +
    theme_minimal(base_size=14) +
    theme(plot.title=element_text(face="bold",size=14),
          plot.subtitle=element_text(size=10, color="gray40"),
          axis.text=element_text(size=12))
}
ggsave("output/accuracy_comparison.png", p_acc, width=10, height=6, dpi=150); cat("  Saved: output/accuracy_comparison.png\n")

# Transition heatmap
p_heat <- { hd<-as.data.frame(transitions[,.N,by=.(esco_sector,next_sector)])
  sh<-function(x) str_trunc(x,18,ellipsis=".."); hd$esco_sector<-sh(hd$esco_sector); hd$next_sector<-sh(hd$next_sector)
  ggplot(hd,aes(x=next_sector,y=esco_sector,fill=log1p(N))) + geom_tile(color="white",linewidth=0.3) +
    scale_fill_gradient(low="#f7fbff",high="#08306b",name="log(count+1)") +
    labs(title="Career Transition Heatmap (27 Sectors)",x="Next Sector",y="Current Sector") +
    theme_minimal(base_size=7)+theme(axis.text.x=element_text(angle=60,hjust=1),plot.title=element_text(face="bold",size=11)) }
ggsave("output/transition_heatmap.png",p_heat,width=14,height=12,dpi=150); cat("  Saved: output/transition_heatmap.png\n")

# Market drift
ddf <- data.frame(window=names(drift_scores),drift=unlist(drift_scores))
p_md <- ggplot(ddf,aes(x=window,y=drift,group=1)) + geom_line(color="steelblue",linewidth=1.2) + geom_point(size=3,color="steelblue") +
  labs(title="Market Drift Over Time (Drifting Markov Model)",subtitle="Frobenius norm between consecutive decade matrices",x="Decade window",y="Drift magnitude") +
  theme_minimal(base_size=12)+theme(axis.text.x=element_text(angle=30,hjust=1),plot.title=element_text(face="bold",size=13))
ggsave("output/market_drift.png",p_md,width=10,height=5,dpi=150); cat("  Saved: output/market_drift.png\n")

# =============================================================================
# FINAL SUMMARY
# =============================================================================
cat("\nFINAL SUMMARY\n")
cat(paste(rep("=",60),collapse=""),"\n")
cat(sprintf("  Persistence Baseline   Top-1: %.2f%%\n", persistence_acc*100))
cat(sprintf("  Ensemble      Top-1: %.2f%%  Top-3: %.2f%%  Top-5: %.2f%%\n",
            ensemble_results["top_1"]*100,ensemble_results["top_3"]*100,ensemble_results["top_5"]*100))
cat(paste(rep("-",60),collapse=""),"\n")
cat(sprintf("  Ensemble vs Baseline : %+.2f%%\n",(ensemble_results["top_1"]-persistence_acc)*100))
cat(paste(rep("=",60),collapse=""),"\n")
cat(paste(rep("=",60),collapse=""),"\n")
for(f in c("sector_distribution.png","sequence_length_distribution.png",
           "pca_sector_profiles.png","market_drift.png","career_drift_distribution.png",
           "top_transitions.png","accuracy_comparison.png","transition_heatmap.png",
           "user_clusters.png","cluster_sector_breakdown.png",
           "association_rules_lift.png","drift_risk_scores.csv","association_rules.csv"))
  cat(sprintf("  - %s\n",f))
