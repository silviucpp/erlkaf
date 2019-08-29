#ifndef C_SRC_CRITICAL_SECTION_H_
#define C_SRC_CRITICAL_SECTION_H_

#include "erl_nif.h"
#include "macros.h"

class CriticalSection
{
public:

    CriticalSection() { mutex_ = enif_mutex_create(NULL);}
    ~CriticalSection() {enif_mutex_destroy(mutex_);}

    void Enter() {enif_mutex_lock(mutex_);}
    void Leave() {enif_mutex_unlock(mutex_);}

private:
    ErlNifMutex *mutex_;

    DISALLOW_COPY_AND_ASSIGN(CriticalSection);
};

class CritScope
{
public:

    explicit CritScope(CriticalSection *pp) : pcrit_(pp) { pcrit_->Enter();}
    ~CritScope() {pcrit_->Leave();}

private:
    CriticalSection *pcrit_;

    DISALLOW_COPY_AND_ASSIGN(CritScope);
};

#endif  // C_SRC_CRITICAL_SECTION_H_
