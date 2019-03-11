/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllerutil

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// AlreadyOwnedError is an error returned if the object you are trying to assign
// a controller reference is already owned by another controller Object is the
// subject and Owner is the reference for the current owner
type AlreadyOwnedError struct {
	Object metav1.Object
	Owner  metav1.OwnerReference
}

func (e *AlreadyOwnedError) Error() string {
	return fmt.Sprintf("Object %s/%s is already owned by another %s controller %s", e.Object.GetNamespace(), e.Object.GetName(), e.Owner.Kind, e.Owner.Name)
}

func newAlreadyOwnedError(Object metav1.Object, Owner metav1.OwnerReference) *AlreadyOwnedError {
	return &AlreadyOwnedError{
		Object: Object,
		Owner:  Owner,
	}
}

// SetControllerReference sets owner as a Controller OwnerReference on owned.
// This is used for garbage collection of the owned object and for
// reconciling the owner object on changes to owned (with a Watch + EnqueueRequestForOwner).
// Since only one OwnerReference can be a controller, it returns an error if
// there is another OwnerReference with Controller flag set.
func SetControllerReference(owner, object metav1.Object, scheme *runtime.Scheme) error {
	ro, ok := owner.(runtime.Object)
	if !ok {
		return fmt.Errorf("is not a %T a runtime.Object, cannot call SetControllerReference", owner)
	}

	gvk, err := apiutil.GVKForObject(ro, scheme)
	if err != nil {
		return err
	}

	// Create a new ref
	ref := *metav1.NewControllerRef(owner, schema.GroupVersionKind{Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind})

	existingRefs := object.GetOwnerReferences()
	fi := -1
	for i, r := range existingRefs {
		if referSameObject(ref, r) {
			fi = i
		} else if r.Controller != nil && *r.Controller {
			return newAlreadyOwnedError(object, r)
		}
	}
	if fi == -1 {
		existingRefs = append(existingRefs, ref)
	} else {
		existingRefs[fi] = ref
	}

	// Update owner references
	object.SetOwnerReferences(existingRefs)
	return nil
}

// Returns true if a and b point to the same object
func referSameObject(a, b metav1.OwnerReference) bool {
	aGV, err := schema.ParseGroupVersion(a.APIVersion)
	if err != nil {
		return false
	}

	bGV, err := schema.ParseGroupVersion(b.APIVersion)
	if err != nil {
		return false
	}

	return aGV == bGV && a.Kind == b.Kind && a.Name == b.Name
}

// OperationResult is the result of a utility operation.
// They should complete the sentence "Deployment default/foo has been ..."
type OperationResult string

const (
	// OperationResultNone means that the resource has not been changed
	OperationResultNone OperationResult = "unchanged"
	// OperationResultCreated means that a new resource is created
	OperationResultCreated OperationResult = "created"
	// OperationResultUpdated means that an existing resource is updated
	OperationResultUpdated OperationResult = "updated"
	// OperationResultDeleted means that an existing resource is deleted
	OperationResultDeleted OperationResult = "deleted"
)

// mutate wraps a MutateFn and applies validation to its result
func mutate(f MutateFn, key client.ObjectKey, obj runtime.Object) error {
	if err := f(); err != nil {
		return err
	}
	if newKey, err := client.ObjectKeyFromObject(obj); err != nil || key != newKey {
		return fmt.Errorf("MutateFn cannot mutate object name and/or object namespace")
	}
	return nil
}

// MutateFn is a function which mutates the existing object into it's desired state.
type MutateFn func() error

// CreateOrUpdateResult is the action result of a CreateOrUpdate call
type CreateOrUpdateResult OperationResult

const (
	// CreateOrUpdateResultNone means that the resource has not been changed
	CreateOrUpdateResultNone = CreateOrUpdateResult(OperationResultNone)
	// CreateOrUpdateResultCreated means that a new resource is created
	CreateOrUpdateResultCreated = CreateOrUpdateResult(OperationResultCreated)
	// CreateOrUpdateResultUpdated means that an existing resource is updated
	CreateOrUpdateResultUpdated = CreateOrUpdateResult(OperationResultUpdated)
)

// CreateOrUpdate creates or updates the given object in the Kubernetes
// cluster. The object's desired state must be reconciled with the existing
// state inside the passed in callback MutateFn.
//
// The MutateFn is called regardless of creating or updating an object.
//
// It returns the executed operation and an error.
func CreateOrUpdate(ctx context.Context, c client.Client, obj runtime.Object, f MutateFn) (CreateOrUpdateResult, error) {
	key, err := client.ObjectKeyFromObject(obj)
	if err != nil {
		return CreateOrUpdateResultNone, err
	}

	if err := c.Get(ctx, key, obj); err != nil {
		if !apierrors.IsNotFound(err) {
			return CreateOrUpdateResultNone, err
		}
		if err := mutate(f, key, obj); err != nil {
			return CreateOrUpdateResultNone, err
		}
		if err := c.Create(ctx, obj); err != nil {
			return CreateOrUpdateResultNone, err
		}
		return CreateOrUpdateResultCreated, nil
	}

	existing := obj.DeepCopyObject()
	if err := mutate(f, key, obj); err != nil {
		return CreateOrUpdateResultNone, err
	}

	if reflect.DeepEqual(existing, obj) {
		return CreateOrUpdateResultNone, nil
	}

	if err := c.Update(ctx, obj); err != nil {
		return CreateOrUpdateResultNone, err
	}
	return CreateOrUpdateResultUpdated, nil
}

// TryUpdateResult is the action result of a TryUpdate or TryUpdateStatus call.
type TryUpdateResult OperationResult

const (
	// TryUpdateResultNone means that the resource has not been changed
	TryUpdateResultNone = TryUpdateResult(OperationResultNone)
	// TryUpdateResultUpdated means that an existing resource is updated
	TryUpdateResultUpdated = TryUpdateResult(OperationResultUpdated)
)

// TryUpdate updates the given object in the Kubernetes
// cluster. The object's desired state must be reconciled with the existing
// state inside the passed in callback MutateFn. If there was a conflict, the given backoff
// will be applied to determine whether and when to retry.
//
// The MutateFn is always called.
//
// It returns the executed operation and an error.
func TryUpdate(ctx context.Context, backoff wait.Backoff, c client.Client, obj runtime.Object, f MutateFn) (TryUpdateResult, error) {
	return tryUpdate(ctx, backoff, c, obj, c.Update, f)
}

// TryUpdateStatus updates the status of the given object in the Kubernetes
// cluster. The object's desired status must be reconciled with the existing
// state inside the passed in callback MutateFn. If there was a conflict, the given backoff
// will be applied to determine whether and when to retry.
//
// The MutateFn is always called.
//
// It returns the executed operation and an error.
func TryUpdateStatus(ctx context.Context, backoff wait.Backoff, c client.Client, obj runtime.Object, f MutateFn) (TryUpdateResult, error) {
	return tryUpdate(ctx, backoff, c, obj, c.Status().Update, f)
}

func sleepBackoff(ctx context.Context, duration time.Duration) error {
	if duration == 0 {
		return ctx.Err()
	}

	timer := time.NewTicker(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func exponentialBackoff(ctx context.Context, backoff wait.Backoff, condition wait.ConditionFunc) error {
	duration := backoff.Duration

	for i := 0; i < backoff.Steps; i++ {
		if ok, err := condition(); err != nil || ok {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			adjusted := duration
			if backoff.Jitter > 0.0 {
				adjusted = wait.Jitter(duration, backoff.Jitter)
			}

			if err := sleepBackoff(ctx, adjusted); err != nil {
				return err
			}
			duration = time.Duration(float64(duration) * backoff.Factor)
		}
	}

	return wait.ErrWaitTimeout
}

func tryUpdate(
	ctx context.Context,
	backoff wait.Backoff,
	c client.Client,
	obj runtime.Object,
	updateFunc func(context.Context, runtime.Object) error,
	f MutateFn,
) (TryUpdateResult, error) {
	key, err := client.ObjectKeyFromObject(obj)
	if err != nil {
		return TryUpdateResultNone, err
	}

	result := TryUpdateResultNone
	err = exponentialBackoff(ctx, backoff, func() (bool, error) {
		if err := c.Get(ctx, key, obj); err != nil {
			return false, err
		}

		beforeTransform := obj.DeepCopyObject()
		if err := mutate(f, key, obj); err != nil {
			return false, err
		}

		if reflect.DeepEqual(obj, beforeTransform) {
			return true, nil
		}

		if err := updateFunc(ctx, obj); err != nil {
			if apierrors.IsConflict(err) {
				return false, nil
			}
			return false, err
		}
		result = TryUpdateResultUpdated
		return true, nil
	})
	return result, err
}

// DeleteIfExistsResult is the action result of a DeleteIfExists call.
type DeleteIfExistsResult = OperationResult

const (
	// DeleteIfExistsResultNone means that the resource has not been changed
	DeleteIfExistsResultNone = DeleteIfExistsResult(OperationResultNone)

	// DeleteIfExistsResultDeleted means that the resource has been deleted
	DeleteIfExistsResultDeleted = DeleteIfExistsResult(OperationResultDeleted)
)

// DeleteIfExists deletes the given object in the Kubernetes
// cluster if it exists.
//
// It returns the executed operation and an error.
func DeleteIfExists(ctx context.Context, c client.Client, obj runtime.Object, opts ...client.DeleteOptionFunc) (DeleteIfExistsResult, error) {
	if err := c.Delete(ctx, obj, opts...); err != nil {
		if apierrors.IsNotFound(err) {
			return DeleteIfExistsResultNone, nil
		}
		return DeleteIfExistsResultNone, err
	}
	return DeleteIfExistsResultDeleted, nil
}
